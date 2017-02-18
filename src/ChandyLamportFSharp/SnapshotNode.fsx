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

(*
    Next goals:
    - Adding neighbors to the collection
    - Heartbeat mechanism to keep active neighbors collection up to date
    - Unique identifiers for new messages
    - Log and history of all messages
*)

/// A node of bunnywolf distributed system.
/// When an instance is created - a TCP server is started listening for requests on specified ipAddress and port.
/// Nodes can exchange messages with each other, keep track of neighbors and be a part of a distributed system.
type SnapshotNode(id, ipAddress, port) =
    member this.n = new NetworkingNode(id, ipAddress, port, this.MessageRoundAbout)

    /// Serves a purpose of message processing dispatcher.
    /// Called by ReceiveMessage function, as soon as the full array of message bytes is received.
    /// Deserializes a message and determines the message data type.
    /// Based on what type of message is received - the approproate processing is applied.
    member this.MessageRoundAbout messageBytes = async {
        let messageObject = getObjectFromBytes messageBytes

        match messageObject with
        | :? Hello as hello ->
            do! this.ReceivedHelloMessage hello
            printfn "Received a hello"
            let sender = {Ip = IPAddress.Parse(hello.address); Port = hello.port}
            if hello.needAck then
                do! this.SendAckTo sender
            do! this.SendHeyaMessageTo sender
        | :? Heya  as heya  ->
            do! this.ReceivedHeyaMessage heya
            printfn "Received a heya"
            if heya.needAck then
                do! this.SendAckTo {Ip = IPAddress.Parse(heya.address); Port = heya.port}
        | :? Ack as ack ->
            printfn "Received ACK: %A" ack.ack
        | _ ->
            do! this.ReceivedInvalidMessage()
            printfn "Received an invalid message"
    }
    member this.Start = this.n.Start