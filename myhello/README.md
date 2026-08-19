# MyHello

## Project 1

### Project Overview

A simple "Hello World" project that demonstrates basic package management. It uses an internal package called `greetings`, which contains a single file (`welcome.go`) responsible for displaying a welcome message.

The project also imports an external module, `rsc.io/quote`, to display the famous Go proverb:

> "Don't communicate by sharing memory, share memory by communicating."

### Structure

The project uses the following architecture:

```text
myhello/
├── greetings/
│   └── welcome.go
├── go.mod
├── go.sum
├── main.go
└── README.md
```

### How to Run

To execute the project, navigate to the `myhello` directory and run:

```bash
go run main.go
```
