# SPL251 Assignment 3 - STOMP Client/Server

A complete STOMP-based publish/subscribe system built for `SPL251`.
This repository includes:

- A **Java server** (`server/`) that supports STOMP 1.2 style frames over TCP.
- A **C++ client** (`client/`) that connects to the server, manages subscriptions, reports events, and generates summaries.

## Project Highlights

- STOMP frame parsing and encoding/decoding.
- Multi-client communication over channels/topics.
- Two server concurrency modes:
  - `tpc` (thread-per-client)
  - `reactor`
- Client command interface for:
  - login/logout
  - join/exit channels
  - reporting events from JSON
  - generating summary files

## Repository Structure

```text
.
|- client/
|  |- src/                # C++ client source code
|  |- include/            # C++ headers
|  |- data/               # sample event input files
|  |- bin/                # client build outputs
|  `- makefile
|- server/
|  |- src/main/java/      # Java server source code
|  |- pom.xml             # Maven configuration
|  `- target/             # Maven build outputs
`- README.md
```

## Prerequisites

### Server

- Java 8+
- Maven 3.6+

### Client

- `g++` with C++11 support
- `make`
- Boost system library
- pthreads

> Note: The provided client `makefile` uses Unix-style commands (`rm`), so building the client is easiest on Linux/macOS/WSL.

## Build Instructions

### 1) Build the Java server

From the repository root:

```bash
cd server
mvn clean package
```

### 2) Build the C++ client

From the repository root:

```bash
cd client
make
```

The executable is created at `client/bin/StompEMIClient`.

## Run Instructions

### Start the server

From `server/`:

```bash
mvn exec:java -Dexec.mainClass="bgu.spl.net.impl.stomp.StompServer" -Dexec.args="7777 tpc"
```

Use `reactor` instead of `tpc` to run in reactor mode:

```bash
mvn exec:java -Dexec.mainClass="bgu.spl.net.impl.stomp.StompServer" -Dexec.args="7777 reactor"
```

### Start the client

From `client/`:

```bash
./bin/StompEMIClient
```

## Client Command Reference

Once the client is running:

- `login {host:port} {username} {password}`
- `join {channel_name}`
- `exit {channel_name}`
- `report {file}`
- `summary {channel_name} {user} {file}`
- `logout`

## Quick Example

```text
login 127.0.0.1:7777 alice pass123
join sports
report data/events1.json
summary sports alice summary.txt
logout
```

## Notes

- Sample event files are available in `client/data/`.
- Summary reports are written to the file path provided in the `summary` command.
- If login fails or protocol validation fails, the server responds with STOMP `ERROR` frames and closes the relevant connection.

## Academic Context

This project was developed as part of **SPL251 - Systems Programming Laboratory**.
It demonstrates socket programming, protocol implementation, concurrency models, and client-server design.
