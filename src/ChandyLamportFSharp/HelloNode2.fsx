#load "HelloNode.fsx"

open HelloNode
open System.Threading
open System.Net

printf "Starting Node2..."
let node2 = HelloNode("node2", IPAddress.Loopback, 7779)
node2.Start

node2.SendHelloMessageTo {Ip = IPAddress.Loopback; Port = 7775} |> Async.Start

Thread.Sleep 60000