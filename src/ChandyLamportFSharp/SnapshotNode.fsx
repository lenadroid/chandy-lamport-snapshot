#load "NetworkingNode.fsx"
#r "../DataContracts/bin/Debug/DataContracts.dll"
#r "../../packages/build/Newtonsoft.Json/lib/net45/Newtonsoft.Json.dll"

open System
open System.Net
open System.Net.Sockets
open DataContracts
open NetworkingNode
open Newtonsoft.Json
open System.Text
open Communication
open System.Collections.Generic

(*
    Termination:
        A node who initiated snapshot knows that it's finished when it has received marker messages through all of its incoming channels.

        To save a state of a snapshot:
            1. Each node will have its state saved on disk
            2. There will be a global Snapshot instance that has the state of all the nodes as soon as it's available

    Each node needs to have fields:
    	- State value
        - State snapshot (which will also indicate if this node has already received a marker message)
        - Messages to snapshot (after a node has already done state snapshot, it needs to persist all the messages in transit as channel state)
        - Incoming channels snapshot
        - Tracking which incoming channels have already sent their back-markers to us
        -

*)

/// This type indicates what the state of nodes would look like.
type Contents = int

/// Message is an alias for a basic message, sending certain content type.
type Message = BasicMessage<Contents>

/// Channel state is a list of messages in transit from one node to another one.
type ChannelState = ResizeArray<Message>

/// Neighbor is a type for naming connected nodes and combining it with their endpoints.
type Neighbor = {
    name: string
    endpoint: ServerEndpoint
}

type ShouldITakeASnapshot = Yes | AlreadyDone

/// A node of bunnywolf distributed system.
/// When an instance is created - a TCP server is started listening for requests on specified ipAddress and port.
/// Nodes can exchange messages with each other, keep track of neighbors and be a part of a distributed system.
/// Snapshot nodes can initiate a snapshot
type SnapshotNode(id, ipAddress, port) =
    let mutable state = 0
    let mutable stateSnapshot = 0
    let mutable shouldTakeASnapshot = Yes

    member this.N = new NetworkingNode(id, ipAddress, port, this.MessageRoundAbout)

    // Indicates whether a node needs to take a local snapshot
    member this.ShouldTakeASnapshot
        with get () = shouldTakeASnapshot
        and set (value) = shouldTakeASnapshot <- value

    /// Current state of current node.
    member this.State
        with get () = state
        and set value = state <- value

    /// Snapshotted state of current node.
    /// It should persisted on disk
    member this.StateSnapshot
        with get () = stateSnapshot
        and set value = stateSnapshot <- value

    // Represents all the nodes current node can reach.
    member val ConnectedNeighbors = new ResizeArray<ServerEndpoint>()

    // Represents a map of nodes and states of channels going from indicated server endpoints to current node.
    member val ChannelStatesSnapshot = new Dictionary<ServerEndpoint, ChannelState> ()

    // Messages received by the current node for which it has already sent an acknowledgement to message sender.
    member val AcknowledgedReceivedMessages = new ResizeArray<Message>()

    // Messages sent by the current node but not yet acknowledged delivery to its destination.
    member val SentButUnacknowledgedMessages = new ResizeArray<Message>()

    (* State operations *)

    member this.HasEnoughToTransfer amount = async {
        return not <| (this.State - amount < 0)
    }

    (* Neighbors *)

    member this.AddNeighbor (endpoint: ServerEndpoint) = async {
        try
            if not <| this.ConnectedNeighbors.Contains(endpoint)
            then this.ConnectedNeighbors.Add(endpoint)
        with
        | e -> printf "Exception adding outgoing channel to node %A:%A %A" endpoint.Ip endpoint.Port e
    }

    (* Snapshots *)

    member this.InitiateSnapshot () = async {
        // Capture own state into StateSnapshot
        // Send snapshot messages to all connected neighbors
        //
        ()
    }

    member this.ReceiveSnapshotMessage () = async {
        ()
    }

    member this.SendSnapshotMessage () = async {
        ()
    }

    member this.ComputeAndPersistChannelState () = async {
        // Compare list of messages sender node sent to us and haven't received an ACK for
        // and a list of messages from sender we have sent an ACK for, and finding the difference.
        // The difference is going to be messages in transit for snapshot.
        ()
    }

    member this.ComputeAndPersistNodeState () = async {
        // Save a value from state into
        ()
    }

    member this.TerminateSnapshotForMyself () = async {
        ()
    }


    (* Basic messages *)

(*
	SendMoney(p, q) ==
	    /\ RuleApplicationCount' = RuleApplicationCount + 1
	    /\ \E m \in 1 .. NodeStates[p] : 
	        /\ MessagesInChannels' = [MessagesInChannels EXCEPT ![p] = [MessagesInChannels[p] EXCEPT ![q] = Append(MessagesInChannels[p][q], m)]]
	        /\ NodeStates' = [NodeStates EXCEPT ![p] = NodeStates[p] - m]
	    /\ UNCHANGED<<MessagesToSnapshot, MarkersSent, NodeStateSnapshots, ChannelStateSnapshots>>
	 
	ReceiveMoney(p, q) ==
	    /\ RuleApplicationCount' = RuleApplicationCount + 1
	    /\ MessagesInChannels[p][q] # <<>>
	    /\ Head(MessagesInChannels[p][q]) # -1
	    /\ NodeStates' = [NodeStates EXCEPT ![q] = NodeStates[q] + Head(MessagesInChannels[p][q])]
	    /\ MessagesInChannels' = [MessagesInChannels EXCEPT ![p] = [MessagesInChannels[p] EXCEPT ![q] = Tail(MessagesInChannels[p][q])]]
	    /\ MessagesToSnapshot' =
	           IF NodeStateSnapshots[q] < 0
	           THEN MessagesToSnapshot
	           ELSE [MessagesToSnapshot EXCEPT ![p] = [MessagesToSnapshot[p] EXCEPT ![q] = Append(MessagesToSnapshot[p][q], Head(MessagesInChannels[p][q]))]]
	    /\ UNCHANGED<<MarkersSent, NodeStateSnapshots, ChannelStateSnapshots>>
*)

    member this.SendBasicMessage (amount: Contents) (node: ServerEndpoint) = async {
        let! invariant = this.HasEnoughToTransfer amount
        if invariant then
            // Extracting the amount from current state.
            this.State <- this.State - amount
            // Preparing a message with amount to send.
            let messageToSend =
                { 
                    id = Guid.NewGuid().ToString()
                    contents = amount
                    address = ipAddress.ToString()
                    port = port
                    needAck = true
                }
            // Putting the message in the list of messages sent, but not yet acknowledged.
            this.SentButUnacknowledgedMessages.Add messageToSend
            // Sending a basic message to known endpoint.
            do! this.N.SendMessageToNeighbor node messageToSend
        else ()
    }

    member this.ReceiveBasicMessage (m: Message) = async {
        let sender = {Ip = IPAddress.Parse(m.address); Port = m.port}
        // Updating the state.
        this.State <- this.State + m.contents
        // If current node has already taken a snapshot of its state,
        // then put received message in the list of messages to snapshot.
        if this.ShouldTakeASnapshot = AlreadyDone then
            this.ChannelStatesSnapshot.[sender].Add(m)
        if m.needAck then
            do! this.AcknowledgeMessage m.id sender
    }


    (* Acknowledgements *)

    member this.AcknowledgeMessage messageId (endpoint: ServerEndpoint) = async {
        ()
    }

    member this.ReceiveAcknowledgement () = async {
        ()
    }

    /// Processing of invalid messages.
    member this.ReceivedInvalidMessage () = async {
        printfn "Received invalid message type."
    }

    /// Serves a purpose of message processing dispatcher.
    /// Called by ReceiveMessage function, as soon as the full array of message bytes is received.
    /// Deserializes a message and determines the message data type.
    /// Based on what type of message is received - the approproate processing is applied.
    member this.MessageRoundAbout messageBytes = async {
        let messageObject = getObjectFromBytes messageBytes

        match messageObject with
        | :? Message as message ->
            do! this.ReceiveBasicMessage message
            printfn "Received a message"
            //do! this.SendHeyaMessageTo sender
        | _ ->
            do! this.ReceivedInvalidMessage()
            printfn "Received an invalid message"

        ()
    }

    member this.Start = this.N.Start