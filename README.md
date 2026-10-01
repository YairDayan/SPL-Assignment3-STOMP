# SPL251 Assignment 3 — Emergency Service (STOMP)

A publish/subscribe platform for emergency channels such as fire, medical, police, and natural disasters. Users subscribe to a channel, report events, and receive updates from everyone on that channel.

The project has two parts:

- A **Java server** that implements a STOMP 1.2 broker. It runs in thread-per-client mode or in reactor mode, chosen at startup.
- A **C++ client** that reads commands from the keyboard, translates them into STOMP frames, and keeps the events it sends and receives so it can write a summary.

All communication follows STOMP 1.2. Frames are text, headers are `key:value` lines, a blank line starts the body, and a null byte ends the frame. Header order does not matter.

## Requirements

Tested on Linux (CS lab or the course Docker).

- **Server:** Java 8+, Maven. Build with Maven from `server/`.
- **Client:** `g++` (C++11), `make`, Boost.System, pthreads.

## Layout

```text
client/
  src/        client sources
  include/    headers, including the provided event parser
  data/       sample event files
  bin/        StompEMIClient after make
  makefile
server/
  pom.xml
  src/main/java/bgu/spl/net/impl/stomp/StompServer.java
```

## Build

Server, from `server/`:

```bash
mvn compile
```

Client, from `client/`:

```bash
make
```

`make` writes the executable to `client/bin/StompEMIClient`.

## Run

From `server/`, thread-per-client:

```bash
mvn exec:java -Dexec.mainClass="bgu.spl.net.impl.stomp.StompServer" -Dexec.args="7777 tpc"
```

Reactor mode:

```bash
mvn exec:java -Dexec.mainClass="bgu.spl.net.impl.stomp.StompServer" -Dexec.args="7777 reactor"
```

The first argument is the port. The second is `tpc` or `reactor`.

From `client/`:

```bash
./bin/StompEMIClient
```

The client reads one command per line from stdin. One thread reads the keyboard and another reads the socket, so incoming `MESSAGE` frames are handled while the user types. Every command except `login` requires an active login. Run one client process per user.

## Client commands

### `login {host:port} {username} {password}`

Opens a TCP connection and sends `CONNECT` with `accept-version:1.2`, `host:stomp.cs.bgu.ac.il`, `login`, and `passcode`.

```text
CONNECT
accept-version:1.2
host:stomp.cs.bgu.ac.il
login:meni
passcode:films

```

| Result | Printed to stdout |
| --- | --- |
| Socket failure | `Could not connect to server` |
| This process is already logged in | `The client is already logged in, log out before trying again` |
| New user, or existing user with the correct password and no active session | `Login successful` |
| Username already has an active session | `User already logged in` |
| Username exists and the password does not match | `Wrong password` |

A successful login is a `CONNECTED` frame with `version:1.2`. A failed login is an `ERROR` frame, after which the server closes the connection. The server remembers the username and password after logout. Subscriptions of that user are removed.

### `join {channel_name}`

Sends `SUBSCRIBE` to `/{channel_name}` with a client-unique `id` and a `receipt`. The channel is created on the server the first time someone subscribes. After `RECEIPT`, the client prints `Joined channel {channel_name}`.

```text
SUBSCRIBE
destination:/fire_dept
id:17
receipt:73

```

From then on, every `MESSAGE` on that channel is parsed and stored, grouped by the reporting user.

### `exit {channel_name}`

Sends `UNSUBSCRIBE` with the subscription `id` and a `receipt`. After `RECEIPT`, the client prints `Exited channel {channel_name}`.

### `report {file}`

1. Parses `{file}` with `parseEventsFile` (`event.h` / `event.cpp`). The file supplies the channel name and the event list.
2. Stores each event locally as a report of the logged-in user, ordered by event time.
3. Sends one `SEND` frame per event to `/{channel_name}`.

The client must already be subscribed to that channel. If it is not, the server returns `ERROR` and closes the connection. The body matches the assignment format:

```text
SEND
destination:/police

user:meni
city:Liberty City
event name:Grand Theft Auto
date time:1762966800
general information:
active:true
forces_arrival_at_scene:false
description:
Pink Lampadati Felon with license plate "STOL3N1". White male 6'2 with black baseball hat.
```

The server forwards the body to every subscriber as a `MESSAGE` frame with `destination`, `subscription` (the subscriber's id), and a server-unique `message-id`.

### `summary {channel_name} {user} {file}`

Writes the events this client has for `{user}` on `{channel_name}` into `{file}`. The file is created if missing, and overwritten if it exists. `{user}` may be the current user. This command does not send a frame.

Events are sorted by `date_time`, then by `event_name`. The description summary is the first 27 characters. If the description is longer, it is followed by `...`. `date time` is the epoch converted to a string such as `29/12/24 22:15`.

```text
Channel police
Stats:
Total: 2
active: 2
forces arrival at scene: 1
Event Reports:
Report_1:
city: Liberty City
date time: 29/12/24 22:15
event name: Grand Theft Auto
summary: Pink Lampadati Felon with l...
```

`Total` is the number of reports. `active` counts reports whose `active` field is true. `forces arrival at scene` counts reports whose `forces_arrival_at_scene` field is true.

### `logout`

Sends `DISCONNECT` with a unique `receipt`, waits for `RECEIPT`, closes the socket, and keeps reading commands. The server drops that user's subscriptions and keeps the username and password.

```text
DISCONNECT
receipt:113

```

## Event file

`report` reads JSON like `client/data/events1_partial.json`:

```json
{
  "channel_name": "police",
  "events": [
    {
      "event_name": "Grand Theft Auto",
      "city": "Liberty City",
      "date_time": 1762966800,
      "description": "Pink Lampadati Felon with license plate \"STOL3N1\".",
      "general_information": {
        "active": true,
        "forces_arrival_at_scene": false
      }
    }
  ]
}
```

`date_time` is a Unix epoch in seconds. Events in the file are in time order. Clients are expected to join the channel before reporting starts.

## Session

Terminal 1, from `server/`:

```bash
mvn exec:java -Dexec.mainClass="bgu.spl.net.impl.stomp.StompServer" -Dexec.args="7777 tpc"
```

Terminal 2, from `client/`:

```text
login 127.0.0.1:7777 meni films
join police
report data/events1_partial.json
summary police meni summary.txt
logout
```

## Server frames

| Frame | When |
| --- | --- |
| `CONNECTED` | `CONNECT` succeeded. Header: `version:1.2`. Empty body. |
| `MESSAGE` | A `SEND` is delivered to a subscriber. |
| `RECEIPT` | A client frame that included `receipt` was processed. Header: `receipt-id`. |
| `ERROR` | The frame is malformed, the user is already logged in, the password is wrong, the client is not subscribed to the `SEND` destination, or the subscription cannot be created. The server then closes the connection. |

`receipt` may be added to any client frame. `DISCONNECT` must include it. A `RECEIPT` means that frame and every earlier frame were received. After `DISCONNECT`, the client closes the socket only once the matching `RECEIPT` arrives.

`ERROR` has a short `message` header. If the failing frame had a `receipt`, the error includes the matching `receipt-id`.
