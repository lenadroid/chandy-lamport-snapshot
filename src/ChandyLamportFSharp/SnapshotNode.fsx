#load "NetworkingNode.fsx"
#r "../DataContracts/bin/Debug/DataContracts.dll"
#r "../../packages/FSharp.Collections.ParallelSeq.1.0.2/lib/net40/FSharp.Collections.ParallelSeq.dll"

open System
open System.Net
open DataContracts
open NetworkingNode
open Communication
open System.Collections.Generic
open FSharp.Collections.ParallelSeq

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

    /// Indicates whether a node needs to take a local snapshot
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

    /// All the nodes current node can reach.
    member val ConnectedNeighbors = new HashSet<ServerEndpoint>()

    /// A map of node endpoints and states of channels going from indicated server endpoints to current node.
    member val ChannelStatesSnapshot = new Dictionary<ServerEndpoint, ChannelState> ()

    // Tracks which incoming nodes have already sent a marker message to current node.
    member val ReceivedMarkerFrom = new Dictionary<ServerEndpoint, bool> ()

    /// Messages received by the current node for which it has already sent an acknowledgement to message sender.
    // member val AcknowledgedReceivedMessages = new ResizeArray<Message>()

    /// Messages sent by the current node but not yet acknowledged delivery to its destination.
    // member val SentButUnacknowledgedMessages = new ResizeArray<Message>()

    (* State operations *)

    /// This method can be generalized to allow a general check if one node can exchange state with another one.
    member this.HasEnoughToTransfer amount = async {
        return this.State - amount >= 0
    }

    (* Neighbors *)

    member this.AddNeighbor (endpoint: ServerEndpoint) = async {
        try
            this.ConnectedNeighbors.Add endpoint |> ignore
        with
        | e -> printf "Exception adding outgoing channel to node %A:%A %A" endpoint.Ip endpoint.Port e
    }

    (* Snapshots TLA+ specification

	   InitiateSnapshot(p) ==
		    /\ RuleApplicationCount' = RuleApplicationCount + 1
		    /\ NodeStateSnapshots[p] < 0 
		    /\ \A q \in Channels[p] : MessagesInChannels' = [MessagesInChannels EXCEPT ![p] = [MessagesInChannels[p] EXCEPT ![q] = Append(MessagesInChannels[p][q], -1)]]
		    /\ MarkersSent' = MarkersSent \cup {p}
		    /\ NodeStateSnapshots' = [NodeStateSnapshots EXCEPT ![p] = NodeStates[p]] 
		    /\ UNCHANGED<<NodeStates, MessagesToSnapshot, ChannelStateSnapshots>>
		 
	   ReceiveMarker(p, q) ==
		    /\ RuleApplicationCount' = RuleApplicationCount + 1
		    /\ MessagesInChannels[p][q] # <<>>
		    /\ ~Head(MessagesInChannels[p][q]) \in Nat
		    /\ NodeStateSnapshots' = IF q \in MarkersSent THEN NodeStateSnapshots ELSE [NodeStateSnapshots EXCEPT ![q] = NodeStates[q]]
		    /\ MarkersSent' = IF q \in MarkersSent THEN MarkersSent ELSE MarkersSent \cup {q} 
		    /\ ChannelStateSnapshots' = 
		           IF q \in MarkersSent 
		           THEN [ChannelStateSnapshots EXCEPT ![p][q] = MessagesToSnapshot[p][q]] 
		           ELSE [ChannelStateSnapshots EXCEPT ![p][q] = <<>>]  
		    /\ MessagesInChannels' = LET nw == [MessagesInChannels EXCEPT ![p] = [MessagesInChannels[p] EXCEPT ![q] = Tail(MessagesInChannels[p][q])]] IN
		           IF q \in MarkersSent 
		           THEN nw 
		           ELSE [nw EXCEPT ![q] = [r \in Channels[q] |-> Append(nw[q][r], -1)]]
		    /\ UNCHANGED<<NodeStates, MessagesToSnapshot>>
	*)

    member this.InitiateSnapshot () = async {
        // Capture own state into StateSnapshot
        // Send snapshot messages to all connected neighbors
        if this.ShouldTakeASnapshot = Yes then
            do! this.PersistNodeState()
            do! this.SendSnapshotMessageToOutgoingChannels()
    }

    member this.ReceiveSnapshotMessage (s: SnapshotMessage) = async {
        let sender = { Ip = IPAddress.Parse(s.address); Port = s.port }

        // Remember, that current node has already received a snapshot message from this sender.
        this.ReceivedMarkerFrom.[sender] <- true

        // In case current node hasn't taken a snapshot yet
        if this.ShouldTakeASnapshot = Yes then

            // Record it's own state.
            do! this.PersistNodeState()

            // Record the state of channel from sender to current node as {empty} set.
            this.ChannelStatesSnapshot.[sender] <- new ChannelState()
            do! this.PersistChannelState sender

            // And send snapshot messages to all outgoing channels.
            do! this.SendSnapshotMessageToOutgoingChannels()

        // In case current node has already recorded its local state
        else
            // We need to record a state of incomming channel from sender to current node,
            // which is a set of messages received after current node recorded its state
            // and before current node received a marker from the sender.
            do! this.PersistChannelState sender

        ()
    }

    member this.SendSnapshotMessageToOutgoingChannels () = async {
        this.ConnectedNeighbors |> PSeq.iter (fun node ->
            async {
                printfn "Sending marker from %A:%A to %A:%A" ipAddress port node.Ip node.Port
                let snapshot =
                    {
                        address = ipAddress.ToString()
                        port = port
                    }
                do! this.N.SendMessageToNeighbor node snapshot
            } |> Async.Start)
    }

    member this.PersistChannelState (sender: ServerEndpoint) = async {
        printfn "Channel state from %A:%A to %A is %A"
            sender.Ip sender.Port id (this.ChannelStatesSnapshot.[sender] |> Seq.toList)
    }

    member this.PersistNodeState () = async {
        this.ShouldTakeASnapshot <- AlreadyDone
        this.StateSnapshot <- this.State
        printfn "Snapshot state of %A IS %A" id (this.StateSnapshot.ToString())
    }

    member this.TerminateSnapshotForMyself () = async {
        ()
    }


    (* Basic messages TLA+ specification

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
                    needAck = false
                }

            // Sending a basic message to known endpoint.
            do! this.N.SendMessageToNeighbor node messageToSend
        else
            printfn "Not enough money to send %d: only %d left" amount this.State
    }

    member this.ReceiveBasicMessage (m: Message) = async {
        let sender = { Ip = IPAddress.Parse m.address; Port = m.port }
       
        // 1. If current node has already taken its own snapshot, but didn't receive a marker from this sender yet:
        //    we need to record current message from this sender into messagesInTransit from sender to our node.

        if this.ShouldTakeASnapshot = AlreadyDone && not <| this.ReceivedMarkerFrom.[sender] then
            this.ChannelStatesSnapshot.[sender].Add m

        // 2. If this node didn't take a snapshot yet:
        //    then it just processes a basic message and doesn't record it into snapshotted state
        //
        // 3. If this node has already taken its own snapshot, and already received a marker from sender:
        //    we shoudl not record current message into messagesInTransit, because it's not a part of snapshotted state,
        //    so we can just do a normal processing on this basic message

        // Hence, message processing is necessary for all cases.
        do! this.ProcessBasicMessage m

        // That means, each node needs to store a list of nodes, who it has already received a marker from.
        // It can be a map with the key of sender endpoint and a value of messagesInTransit

    }

    member this.ProcessBasicMessage (m: Message) = async {
        // Updating the state.
        this.State <- this.State + m.contents
        printfn "Received %A dollars, now balance is %A" m.contents this.State
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