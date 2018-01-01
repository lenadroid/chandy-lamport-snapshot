#r "../../packages/build/Newtonsoft.Json/lib/net45/Newtonsoft.Json.dll"

open System.Net
open System.Net.Sockets
open System.Text
open Newtonsoft.Json

let settings = JsonSerializerSettings(TypeNameHandling = TypeNameHandling.All)

/// Accepts a byte array and returns an object as a result it's decerialization.
let getObjectFromBytes (messageBytes: byte array) =
    JsonConvert.DeserializeObject(Encoding.ASCII.GetString messageBytes, settings)

/// Accepts an object and serializes it into bytes, preserving type information.
let getBytes (message:'a) =
        Encoding.ASCII.GetBytes(JsonConvert.SerializeObject(message, settings))

type ServerEndpoint = { Ip: IPAddress; Port: int }
type NodeId = string * ServerEndpoint

type AsyncTcpServer(address, port, processServerRequest) =
        let listener = TcpListener(address, port)
        member this.Start() =
            async {
                do this.Run()
            } |> Async.Start

        member this.Run() =
            printfn "Started a TCP server on port %A..." port
            listener.Start()
            while true do
                let client = listener.AcceptTcpClient()
                async {
                    try
                        do! processServerRequest client
                    with e -> ()
                } |> Async.Start