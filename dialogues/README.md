# Dialogues

## Project 2

### Project Overview

An interprocess communication (IPC) project that simulates client-server message exchange via the UDP protocol.

Each process (except in the `oneway` scenario) receives as arguments the port on which it will listen for messages, followed by the ports of the other processes to which it will send messages. The project is divided into four incremental stages:

1. **Oneway**: A basic one-way pipe with separate client and server processes. The client can only send messages, and the server can only receive them.
2. **Twoway**: A single peer node that acts as both a client and a server simultaneously. It receives messages via its server side and sends messages via its client side, leveraging Go routines to handle concurrent, lightweight threads.
3. **Controlled**: Adds user control to the two-way process. Message transmission is now triggered asynchronously via `stdin` (keyboard input) using Go channels.
4. **Structured**: Adds a defined payload structure to the controlled process, exchanging serialized structures rather than raw strings.

### Structure

The project uses the following architecture:

```text
dialogues/
├── controlled/
│   └── process.go
├── oneway/
│   ├── client.go
│   └── server.go
├── structured/
│   └── process.go
├── twoway/
│   └── process.go
└── README.md
```

### How to Run

To execute the project, navigate to one of the multi-node directories (`controlled`, `structured`, `twoway`) and launch the processes in separate terminal instances.

For a three-node setup, run:

Terminal 1:

```bash
go run process.go :10001 :10002 :10003
```

Terminal 2:

```bash
go run process.go :10002 :10001 :10003
```

Terminal 3:

```bash
go run process.go :10003 :10001 :10002
```

> Note: To add additional processes, simply append their ports to the previous commands. You can change the port numbers to any available ports on your local machine.

For the `oneway` directory, open two terminals and run:

Terminal 1 (Server):

```bash
go run server.go
```

Terminal 2 (Client):

```bash
go run client.go
```
