#load "SnapshotNode.fsx"

open SnapshotNode
open System.Threading
open System.Net

printf "Starting SnapshotNode3..."
let node3 = SnapshotNode("node3", IPAddress.Loopback, 7773, 0, 0)

node3.Start

node3.AddNeighbor {Ip = IPAddress.Loopback; Port = 7771} |> Async.RunSynchronously
node3.AddNeighbor {Ip = IPAddress.Loopback; Port = 7772} |> Async.RunSynchronously
Async.Sleep 10000 |> Async.RunSynchronously

Async.Sleep 60000 |> Async.RunSynchronously

