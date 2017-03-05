#r "../../packages/FSharp.Collections.ParallelSeq.1.0.2/lib/net40/FSharp.Collections.ParallelSeq.dll"

open System
open System.Drawing
open System.Collections
open System.Collections.Generic
open FSharp.Collections.ParallelSeq

// Definition of fields for a basic message
type MessageID = string
type SenderAddress = string
type DestinationAddress = string
type Data = string

type BasicMessage = {
    id: MessageID;
    sender: SenderAddress;
    destination: DestinationAddress;
    data: Data
}

// Definition of a marker message
// It may or may not contain a sender address, depending on:
// 1. Whether marker was sent from a node that had already received a marker message in the past
// 2. Or whether a marker was sent by a system to initiate Snapshot process for entire node cluster
type Marker = Option<SenderAddress>

// A message can be either a basic message or a marker
type Message = Marker | BasicMessage

// Types used for storing Node's state and Snapshot state
type State = Color
type ShouldITakeASnapshot = Yes | AlreadyDone

type Node(initialState: State, name: string) =

    let mutable shouldITakeASnapshot = Yes

    // Indicates whether a node needs to take a local snapshot
    member this.ShouldITakeASnapshot
        with get () = shouldITakeASnapshot
        and set (value) = shouldITakeASnapshot <- value

    // Name of the node, like "P", "Q" or "R" in this example
    member val Name = name

    // State is a color, like Purple, Orange or Yellow
    // Every node is in some certain initial state
    member val State = initialState

    // Returns the address of the node who sent the marker
    member this.GetASenderAddressOfMarker(marker: Marker) =
        match marker with
        | None -> failwith "Nobody sent this marker"
        | Some(m) -> m

    // Represents all the nodes current node has inbound channels to.
    member val ReachableNeighbors = new ResizeArray<Node>()

    // Messages received by the current node but not yet acknowledged or processed.
    // This collection is initially empty.
    member val MessagesReceived = new ResizeArray<BasicMessage>()

    // Messages sent by the current node but not yet acknowledged delivery to its destination.
    // This collection is initially empty.
    member val MessagesSent = new ResizeArray<BasicMessage>()

    // Returns a Node based on its name
    // TODO: need to implement this function to return a node based on its real endpoint address
    member this.GetNodeByAddress(address:string) =
        this.ReachableNeighbors |> PSeq.find (fun n -> n.Name = address)

    // Print state of a node and mark the snapshot as already done
    member this.TakeLocalSnapshot() =
        async {
            this.ShouldITakeASnapshot <- AlreadyDone
            printfn "SNAPSHOT OF %A IS %A" this.Name (this.State.ToString())
        } |> Async.Start

    // Checks if the Snapshot process has been initiated
    member this.IsItSnapshotInit(marker: Marker) =
        match marker with
        | None -> true
        | _ -> false

    // Gets channel state: messages that are in sender's outbox but not in receiver's inbox yet
    //
    // This function should change
    // We should have a log on each node, which should state operations happenning and those that have happened
    // So if we sent a message and haven't received
    member this.CaptureChannelState(senderAddress:string) =
        async {
            // To get all basic messages the sender sent to current node
            let senderSentMessageIDs =
                this.GetNodeByAddress(senderAddress).MessagesSent
                // filter to include only messages sent to our node
                |> PSeq.filter (fun m -> m.destination = this.Name)
                // get ids of messages directed to us
                |> PSeq.map (fun m -> m.id)
            // To get all basic messages this node has received from sender
            let thisReceivedMessageIDs =
                this.MessagesReceived
                // filter to include only messages sent from the correct sender
                |> PSeq.filter (fun m -> m.sender = senderAddress)
                // get ids of messages received from sender
                |> PSeq.map (fun m -> m.id)
            let inTransit = Set.ofSeq(senderSentMessageIDs) - Set.ofSeq(thisReceivedMessageIDs)
            printfn "CHANNEL STATE OF %A%A is {%A}"
                senderAddress
                this.Name
                (inTransit |> Set.fold (fun acc m -> acc + m + " ") String.Empty)
        } |> Async.Start

    // When a marker is received there are three cases
    // 1. The marker is an initiator of a snapshot.
    // 2. The marker is a result of another node reseiving a marker, therefore sending a marker to its outgoing channels. Snapshot has already started.
    // 3. The marker is received by the node as a result of previously sending a marker to the node that has sent it.
    member this.ReceiveMarker(marker: Marker) =
        async {
            do! Async.Sleep((new Random()).Next(5000))
            if (this.IsItSnapshotInit(marker)) then
                printfn "Received a start snapshot at %A" this.Name
                this.TakeLocalSnapshot()
                this.SendMarkerToOutgoingChannels()
            elif (this.ShouldITakeASnapshot = Yes) then
                printfn "Node %A received a marker from %A first time"
                    this.Name
                    (this.GetASenderAddressOfMarker(marker))
                this.SendMarkerToOutgoingChannels()
                this.TakeLocalSnapshot()
                this.CaptureChannelState(this.GetASenderAddressOfMarker(marker))
            else
                printfn "Node %A received a marker from node %A back"
                    this.Name
                    (this.GetASenderAddressOfMarker(marker))
                this.CaptureChannelState(this.GetASenderAddressOfMarker(marker))
        } |> Async.Start

    member this.SendMarkerToOutgoingChannels() =
        this.ReachableNeighbors
        |> PSeq.iter (fun n ->
            async {
                printfn "Sending marker from %A to %A" this.Name n.Name
                n.ReceiveMarker(Some(this.Name))
            } |> Async.Start)

    member this.SendBasicMessage(message: BasicMessage) =
        // Add message to this nodes outbox
        this.MessagesSent.Add(message)
        printfn "Sent a basic message from %A to %A" message.sender message.destination
        // Trigger the receive on the destination node
        this.GetNodeByAddress(message.destination).ReceiveBasicMessage(message)

    member this.ReceiveBasicMessage(message: BasicMessage) =
        async {
            // Add new message to this nodes inbox
            Async.Sleep(50000) |> Async.RunSynchronously
            this.MessagesReceived.Add(message)
            printfn "Received a basic message from %A to %A" message.sender message.destination
        } |> Async.Start

    member this.StartSnapshot() =
        this.ReceiveMarker(None)

    member this.AddOutNode(node) =
        try
            if not <| this.ReachableNeighbors.Contains(node)
            then this.ReachableNeighbors.Add(node)
        with
        | e -> printf "Exception adding outgoing channel to node %A %A" node.Name e

    override this.Equals(obj) =
        match obj with
        | :? Node as y -> (this.Name = y.Name && this.State = y.State)
        | _ -> false

    override this.GetHashCode() = hash this.Name

    interface IComparable with
        member this.CompareTo y =
            match y with
            | :? Node as y -> compare this.Name y.Name
            | _ -> invalidArg "y" "cannot compare values of different types"

let prepareBasicMessage(id: string, sender: Node, destination: Node, data: Data) =
    {
        id = id
        sender = sender.Name
        destination = destination.Name
        data = data
    }

let P = new Node(Color.Purple, "P")
let Q = new Node(Color.Orange, "Q")
let R = new Node(Color.Yellow, "R")

P.AddOutNode Q
P.AddOutNode R
Q.AddOutNode R
Q.AddOutNode P
R.AddOutNode P
R.AddOutNode Q

let m2 = prepareBasicMessage("m2", Q, R, "hello!")
Q.SendBasicMessage(m2)

P.StartSnapshot()
