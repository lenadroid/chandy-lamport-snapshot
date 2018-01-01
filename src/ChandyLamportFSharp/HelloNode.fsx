#load "NetworkingNode.fsx"
#r "../DataContracts/bin/Debug/DataContracts.dll"
#r "../../packages/build/Newtonsoft.Json/lib/net45/Newtonsoft.Json.dll"

open System.Net
open DataContracts
open NetworkingNode
open Communication

/// A node of bunnywolf distributed system.
/// When an instance is created - a TCP server is started listening for requests on specified ipAddress and port.
/// Nodes can exchange messages with each other, keep track of neighbors and be a part of a distributed system.
type HelloNode(id, ipAddress, port) =
    member this.n = NetworkingNode(id, ipAddress, port, this.MessageRoundAbout)

    /// Sends a Hello message to a specified server endpoint.
    member this.SendHelloMessageTo(neighbor: ServerEndpoint) = async {
        let helloMessage = { hello = "Hello"; address = ipAddress.ToString(); port = port; needAck = true }
        do! this.n.SendMessageToNeighbor neighbor helloMessage
    }

    /// Sends a Heya message to a specified server endpoint.
    member this.SendHeyaMessageTo(neighbor: ServerEndpoint) = async {
        let heya = { heya = "Heya"; address = ipAddress.ToString(); port = port; needAck = true }
        do! this.n.SendMessageToNeighbor neighbor heya
    }

    /// Sends an acknowledgement to a specified server endpoint.
    member this.SendAckTo (neighbor: ServerEndpoint) = async {
        let ack = {ack = "acknowledgement"}
        do! this.n.SendMessageToNeighbor neighbor ack
    }

    /// Processing of Heya messages.
    member this.ReceivedHeyaMessage (message: Heya) = async {
        printfn "Heya message received: %A" message.heya
    }

    /// Processing of Hello messages.
    member this.ReceivedHelloMessage (message: Hello) = async {
        printfn "Hello message received: %A" message.hello
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