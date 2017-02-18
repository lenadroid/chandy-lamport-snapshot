#load "Communication.fsx"
#r "../DataContracts/bin/Debug/DataContracts.dll"

open System
open System.Net
open System.Net.Sockets
open Communication

type NetworkingNode (id, ipAddress, port, roundabout) =

    /// Collection of active neighbors of current node.
    member val neighbors = new ResizeArray<string*ServerEndpoint>()

    /// Opens a new connection with a Node based on the given server endpoint and transfers the logic to SendMessage function.
    member this.SendMessageToNeighbor(neighbor: ServerEndpoint) (message:obj) = async {
        // Connect to the server
        use client = new TcpClient()
        client.Connect(neighbor.Ip, neighbor.Port)
        use stream = client.GetStream()
        // Write a message
        do! this.SendMessage message stream
    }

    /// Starts a TCP server to listen for requests.
    member this.Start =
        let server = AsyncTcpServer(ipAddress, port, this.ReceivedConnectRequest)
        server.Start()
        printfn "Started a server listening on %A:%A" ipAddress port

    /// This function is called as soon as there is a new request to current Node.
    /// It adds the sender into the collection of neighbors if it's not there already.
    /// Finally it transfers message processing to ReceiveMessage function.
    member this.ReceivedConnectRequest (client: TcpClient) = async {
        let clientAddress = box client.Client.RemoteEndPoint :?> IPEndPoint

        // How to manage neighbors?
        // this.neighbors.Add(clientAddress)

        let stream = client.GetStream()
        do! this.ReceiveMessage stream
    }

    /// Called by ReceivedConnectRequest.
    /// Receives a stream of data from sender Node.
    /// The first 4 bytes received is a size of a message.
    /// Rest of the bytes are read untill the message size is reached.
    member this.ReceiveMessage (stream: NetworkStream) = async {
        // uy - unsigned 8-bit natural number
        try
            let sizeBytes = Array.create 4 0uy
            let! readSize = stream.AsyncRead(sizeBytes, 0, 4)
            let size = BitConverter.ToInt32(sizeBytes, 0)
            let dataBuffer = Array.create size 0uy
            let! bytesReceived = stream.AsyncRead(dataBuffer, 0, size)
            if bytesReceived = 0 then printfn "Zero bytes received."
            else do! roundabout dataBuffer
        with
        | :? System.ObjectDisposedException ->
            printfn "Connection is closed."
    }
    /// Writes an object into a stream.
    member this.SendMessage message (stream: NetworkStream) = async {
        do! Async.Sleep 1000
        let messageBytes = getBytes message
        let size = messageBytes.Length
        let sizeBytes = BitConverter.GetBytes size
        // Write message size to network stream
        do! stream.AsyncWrite(sizeBytes, 0, sizeBytes.Length)
        // Write a message to network stream
        do! stream.AsyncWrite(messageBytes, 0, messageBytes.Length)
    }