namespace DataContracts

type Heya =
    {
        heya: string
        address: string
        port: int
        needAck: bool
    }

type Hello =
    {
        hello: string
        address: string
        port: int
        needAck: bool
    }

type Ack = {ack: string}

type Marker = {marker: string}

/// Basic message is a type used for communication between nodes.
/// Each basic message has a unique ID.
/// Address and port are sender's endpoint for receiving messages.
type BasicMessage<'a> = {
    id: string
    contents: 'a
    address: string
    port: int
    needAck: bool
    delay: int
}

/// Snapshot message is a marker message to indicate that a node needs to
/// take a snapshot of its state and state of its incoming channels.
type SnapshotMessage = {
    address: string
    port: int
}

/// Acknowledgement is necessary to confirm that a certain message was successfully received by some node
type Acknowledgement = {
    messageId: string
}