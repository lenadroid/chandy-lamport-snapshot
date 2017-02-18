#load "HelloNode.fsx"

open HelloNode
open System.Threading
open System.Net


printf "Starting Node1..."
let node1 = HelloNode("node1", IPAddress.Loopback, 7775)
node1.Start

Thread.Sleep 60000

