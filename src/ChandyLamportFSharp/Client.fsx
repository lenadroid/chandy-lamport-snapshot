#load "AaTcpServer.fsx"
open System.Threading
open System.IO
open System
open System.Net
open System.Net.Sockets
open System.Text
open AaTcpServer

module Client =

    let ReceiveMessage(message: byte array) (stream: NetworkStream) =
        async {
            // uy - unsigned 8-bit natural number
            let buffer = Array.create message.Length 0uy
            try
                let! read = stream.AsyncRead(buffer, 0, message.Length)
                if read = 0 then
                    return None
                else
                    let receivedMessage = Encoding.ASCII.GetString buffer
                    return Some(receivedMessage)
            with
            | :? System.ObjectDisposedException ->
                printfn "Connection is closed."
                return None
        }

    let Disconnect (stream:NetworkStream, client: TcpClient) =
        printfn "Disconnecting..."
        stream.Close()
        client.Close()

    let ReceiveMessages (stream:NetworkStream) = async {
            let receive = async {
                let! receivedMessage = ReceiveMessage message stream
                match receivedMessage with
                | None ->
                    printfn "Nothing to receive"
                    //stream |> Disconnect
                    return None
                | Some value ->
                    printfn "Received a message: %A!" value
                    return Some value
            }
            let rec loopAround = async {
                let! result = receive
                match result with
                | None -> return ()
                | _ -> do! loopAround
            }
            return! loopAround
        }

    let Connect(serverEndpoit: ServerEndpoint) =
        async {
            let client = new TcpClient()
            client.Connect(serverEndpoit.Ip, serverEndpoit.Port)
            printfn "Connected!"

            let stream = client.GetStream()

            printfn "Got stream!"
            return stream
            (*let! welcomeMessage = ReceiveMessage header stream
            match welcomeMessage with
                | None ->
                    stream |> Disconnect
                    return ()
                | _ -> ()

            do! ReceiveMessages stream*)
        }

async {
    let! stream = Client.Connect {Ip = IPAddress.Parse("127.0.0.1"); Port = 9090}
    return ()
}