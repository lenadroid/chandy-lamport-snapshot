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
open System.Threading

type Atom<'T when 'T : not struct>(value : 'T) =
    let refCell = ref value

    let rec swap f =
        let currentValue = !refCell
        let result = Interlocked.CompareExchange<'T>(refCell, f currentValue, currentValue)
        if obj.ReferenceEquals(result, currentValue) then result
        else Thread.SpinWait 20; swap f

    member self.Value with get() = !refCell
    member self.Swap (f : 'T -> 'T) = swap f

let atom value =
    new Atom<_>(value)

let (!) (atom : Atom<_>) =
    atom.Value

let swap (atom : Atom<_>) (f : _ -> _) =
    atom.Swap f


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

/// When an instance is created - TCP server is started listening for requests on specified ipAddress and port.
/// Nodes can exchange messages with each other, keep track of neighbors and be a part of a distributed system.
/// Snapshot nodes can initiate a snapshot
type SnapshotNode(id, ipAddress, port, initState, delay) =
    let state = initState
    let mutable stateSnapshot = 0
    let mutable shouldTakeASnapshot = Yes

    member this.N = NetworkingNode(id, ipAddress, port, this.MessageRoundAbout)

    /// Indicates whether a node needs to take a local snapshot
    member this.ShouldTakeASnapshot
        with get () = shouldTakeASnapshot
        and set (value) = shouldTakeASnapshot <- value

    /// Snapshotted state of current node.
    /// It should persisted on disk
    member this.StateSnapshot
        with get () = stateSnapshot
        and set value = stateSnapshot <- value

    member val StateAtom = atom (fun () -> state)

    /// All the nodes current node can reach.
    member val ConnectedNeighbors = new HashSet<ServerEndpoint>()

    /// A map of node endpoints and states of channels going from indicated server endpoints to current node.
    member val ChannelStatesSnapshot = new Dictionary<ServerEndpoint, ChannelState> ()

    // Tracks which incoming nodes have already sent a marker message to current node.
    member val ReceivedMarkerFrom = new Dictionary<ServerEndpoint, bool> ()

    (* State operations *)

    /// This method can be generalized to allow a general check if one node can exchange state with another one.
    member this.HasEnoughToTransfer amount = async {
        //return this.State - amount >= 0
        return (!this.StateAtom)() - amount >= 0
    }

    (* Neighbors *)

    member this.AddNeighbor (endpoint: ServerEndpoint) = async {
        try
            printfn "Adding %d as neighbor" endpoint.Port
            this.ChannelStatesSnapshot.[endpoint] <- new ChannelState()
            this.ConnectedNeighbors.Add endpoint |> ignore
        with
        | e -> printf "Exception adding outgoing channel to node %A:%A %A" endpoint.Ip endpoint.Port e
    }

    member this.InitiateSnapshot () = async {
        // Capture own state into StateSnapshot
        // Send snapshot messages to all connected neighbors
        printfn "Initiating a snapshot!"
        if this.ShouldTakeASnapshot = Yes then
            do! this.PersistNodeState()
            do! this.SendSnapshotMessageToOutgoingChannels()
    }

    member this.ReceiveSnapshotMessage (s: SnapshotMessage) = async {
        let sender = { Ip = IPAddress.Parse s.address; Port = s.port }

        // Remember, that current node has already received a snapshot message from this sender.
        this.ReceivedMarkerFrom.[sender] <- true

        printf "Received a marker message from %s:%d" s.address s.port

        // In case current node hasn't taken a snapshot yet
        if this.ShouldTakeASnapshot = Yes then
            // Record it's own state.
            do! this.PersistNodeState()

            // Record the state of channel from sender to current node as {empty} set.
            do! this.PersistChannelState sender

            // And send snapshot messages to all outgoing channels.
            do! this.SendSnapshotMessageToOutgoingChannels()

        // In case current node has already recorded its local state by
        // receiving a snapshot message earlier from some other neighbor
        else
            // We need to record a state of incomming channel from sender to current node,
            // which is a set of messages received after current node recorded its state
            // and before current node received a marker from the sender.
            do! this.PersistChannelState sender
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

    member this.PersistNodeState () = async {
        this.ShouldTakeASnapshot <- AlreadyDone
        this.StateSnapshot <- (!this.StateAtom)()
        Console.ForegroundColor <- ConsoleColor.Blue
        printfn "*** Snapshot state of %A IS %A ***" id (this.StateSnapshot.ToString())
        Console.ForegroundColor <- ConsoleColor.White
    }

    member this.PersistChannelState (sender: ServerEndpoint) = async {
        Console.ForegroundColor <- ConsoleColor.Blue
        printfn "*** Channel state from %A:%A to %A is %A ***"
            sender.Ip sender.Port id (this.ChannelStatesSnapshot.[sender] |> Seq.toList)
        Console.ForegroundColor <- ConsoleColor.White
    }

    member this.SendBasicMessage (amount: Contents) (delay: int) (node: ServerEndpoint) = async {

        let s = (!this.StateAtom)()
        printfn "Want to send %d, and my state is %d" amount s

        let! invariant = this.HasEnoughToTransfer amount
        if invariant then
            // Extracting the amount from current state.
            swap this.StateAtom
                (fun f ->
                    (fun result () ->
                        if (result - amount >= 0) then result - amount else result
                    ) <| f()
                ) |> ignore

            // Preparing a message with some amount to send.
            let messageToSend =
                {
                    id = Guid.NewGuid().ToString()
                    contents = amount
                    address = ipAddress.ToString()
                    port = port
                    needAck = false
                    delay = 0
                }
            printfn "Money sent %d, money left %d" amount ((!this.StateAtom)())

            // Sending a basic message to known endpoint.
            do! this.N.SendMessageToNeighbor node messageToSend
        else
            printfn "Not enough money to send %d: only %d left" amount s
    }

    member this.ReceiveBasicMessage (m: Message) = async {
        let sender = { Ip = IPAddress.Parse m.address; Port = m.port }

        // 1. If current node has already taken its own snapshot, but didn't receive a marker from this sender yet:
        //    we need to record current message from this sender into messagesInTransit from sender to our node.
        printfn "Already received a marker from %A: %A!" sender (this.ReceivedMarkerFrom.ContainsKey(sender))
        if this.ShouldTakeASnapshot = AlreadyDone && not <| this.ReceivedMarkerFrom.ContainsKey(sender) then
            printfn "Adding message $%d from %s:%d to the channel state, because this node has already taken it's local snapshot, but haven't received a marker from sender." m.contents m.address m.port
            this.ChannelStatesSnapshot.[sender].Add m
        else printfn "Received %d" m.contents

        // 2. If this node didn't take a snapshot yet:
        //    then it just processes a basic message and doesn't record it into snapshotted state
        //
        // 3. If this node has already taken its own snapshot, and already received a marker from sender:
        //    we should not record current message into messagesInTransit, because it's not a part of snapshotted state,
        //    so we can just do a normal processing on this basic message

        // Hence, message processing is necessary for all cases.
        do! this.ProcessBasicMessage m

        // That means, each node needs to store a list of nodes, who it has already received a marker from.
        // It can be a map with the key of sender endpoint and a value of messagesInTransit

    }

    member this.ProcessBasicMessage (m: Message) = async {
        // Updating the state.
        swap this.StateAtom
            (fun f ->
                (fun result () ->
                    result + m.contents
                ) <| f()
            ) |> ignore
        let s = (!this.StateAtom)()
        printfn "Received %A dollars, now balance is %A" m.contents s // this.State
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
        // printfn "In snapshot message roundabout!"
        match getObjectFromBytes messageBytes with
        | :? Message as message ->
            // Emulating network latency delay...
            do! Async.Sleep 1000

            do! this.ReceiveBasicMessage message

        | :? SnapshotMessage as snapshot ->

            // Emulating network latency delay...
            if id = "node2" then
               do! Async.Sleep (7000)

            do! this.ReceiveSnapshotMessage snapshot

        | _ ->
            do! this.ReceivedInvalidMessage()
            printfn "Received an invalid message"
    }

    member this.Start = this.N.Start