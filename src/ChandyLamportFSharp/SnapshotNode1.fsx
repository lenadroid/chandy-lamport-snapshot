#load "SnapshotNode.fsx"

open SnapshotNode
open System.Net


printf "Starting SnapshotNode1..."
let node1 = SnapshotNode("node1", IPAddress.Loopback, 7771, 50, 0)

node1.Start
node1.AddNeighbor {Ip = IPAddress.Loopback; Port = 7772} |> Async.RunSynchronously
node1.AddNeighbor {Ip = IPAddress.Loopback; Port = 7773} |> Async.RunSynchronously
Async.Sleep 10000 |> Async.RunSynchronously

printfn "Sending a $5 to node2!"

node1.SendBasicMessage 5 0 {Ip = IPAddress.Loopback; Port = 7772} |> Async.RunSynchronously

printfn "Initiating a snapshot!"

node1.InitiateSnapshot() |> Async.RunSynchronously


Async.Sleep 60000 |> Async.RunSynchronously

