#load "SnapshotNode.fsx"

open SnapshotNode
open System.Threading
open System.Net

printf "Starting SnapshotNode2..."
let node2 = SnapshotNode("node2", IPAddress.Loopback, 7772, 70, 1000)

node2.Start
node2.AddNeighbor {Ip = IPAddress.Loopback; Port = 7771} |> Async.RunSynchronously
node2.AddNeighbor {Ip = IPAddress.Loopback; Port = 7773} |> Async.RunSynchronously
Async.Sleep 12000 |> Async.RunSynchronously

printfn "Sending a $10 to node3 with 1 seconds delay!"

node2.SendBasicMessage 10 0 {Ip = IPAddress.Loopback; Port = 7773} |> Async.RunSynchronously


Async.Sleep 60000 |> Async.RunSynchronously

