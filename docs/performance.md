# Performance

How fast the bouncer is under many clients, how that was measured, and what
limits it. The short of it:

- A replica asked by one client at a time answers about 50 thousand trivial
  requests a second on the machine below; asked by ten or more clients at
  once, about 100 thousand, because it then reads several requests for each
  time it wakes.
- The bouncer gives a replica connection one request at a time, so each
  connection is the first case, plus the bouncer's own turn-around: 30 to 34
  thousand trivial requests a second for one replica, 39 to 44 thousand for
  two, 46 to 58 thousand for four.
- `slots = 4` gives a replica four connections and so the second case: two
  replicas then answer 73 thousand trivial requests a second, and 80 thousand
  with `slots = 16`.
- For requests that cost the replica a millisecond or more the bouncer is not
  what limits anything: it costs about 4% on one replica, and two and four
  replicas do the work of two.
- Clients that are refused with `busy` and ask again at once are the one load
  that is still served badly. See [Refusals](#refusals).

## Running the benchmark

`rfbench` is built with the bouncer. It speaks to a replica and to the
bouncer alike, so the two can be compared.

```sh
cargo build --release
./target/release/rfbench 127.0.0.1:5000 --clients 1000 --duration 10s
./target/release/rfbench 127.0.0.1:5001 --clients 1000 --password replica-secret   # a replica
./target/release/rfbench 127.0.0.1:5000 --clients 100 --reconnect -n 5000          # a connection per request
./target/release/rfbench 127.0.0.1:5000 --query '(sum (til 1000000))' --wire 3
```

```
target 127.0.0.1:5000  clients 1000  persistent connections  wire 4  query "(+ 1 2)"
elapsed_s 3.040  ok 78540  errors 0
throughput_rps 25833.0
latency_us  p50 38687  p95 42118  p99 67207  max 68829
```

Every client sends its next request when the last one is answered. The
throughput is of requests answered with a value; anything else is listed by
kind under it (`error  12  error: busy`, `connect: ...`, `greeting: ...`,
`send`, `receive`, `timed out`). With `--reconnect` the latency includes the
connect and the greeting. It exits with a failure only when nothing at all
was answered.

Two things to know before a large run:

- Give the pool a `queue_max` above the number of clients, or the clients
  past it are answered `busy` and the run measures refusals.
- Connections are closed with a reset, so their ports are free at once. A
  run with `--reconnect` still opens connections as fast as the server takes
  them: on a machine others use, keep it short with `-n`.

## The numbers

Machine: Apple M4, 10 cores, macOS 26 (Darwin 25.6), everything on loopback:
the replicas (`rayforce -p PORT -U pw -c 1`, a development build that speaks
wire version 4), the bouncer and `rfbench`. The machine was running other
work throughout, with a load average between 4 and 13, so every number is
the median of three runs of two seconds, with the least and the most beside
it where they matter, and "before" and "after" were run in turns within the
same minute. The pool had `queue_max = 100000`. "Before" is commit 418a204.

Trivial requests, `(+ 1 2)`, persistent connections, requests a second:

| Replicas | Clients | A replica directly | Before | After | CPU a request, before | after |
|---|---|---|---|---|---|---|
| 1 | 1 | 50 600 | 23 500 | 24 100 | 22 µs | 19 µs |
| 1 | 10 | 101 300 | 31 800 | 33 900 | 30 µs | 26 µs |
| 1 | 100 | 105 600 | 31 300 | 34 000 | 32 µs | 26 µs |
| 1 | 1000 | 102 700 | 28 700 | 33 500 | 39 µs | 30 µs |
| 1 | 3000 | 95 500 | 22 100 | 30 000 | 73 µs | 48 µs |
| 2 | 10 | | 45 200 | 39 900 | 33 µs | 33 µs |
| 2 | 100 | | 20 400 (15 200..42 700) | 32 900 (30 400..46 400) | 67 µs | 34 µs |
| 2 | 1000 | | 37 300 | 43 600 | 47 µs | 33 µs |
| 2 | 3000 | | 28 900 | 39 100 | 81 µs | 49 µs |
| 4 | 10 | | 55 300 | 57 700 | 45 µs | 37 µs |
| 4 | 100 | | 41 400 | 51 300 | 57 µs | 40 µs |
| 4 | 1000 | | 46 100 | 52 200 | 59 µs | 42 µs |
| 4 | 3000 | | 32 100 (22 700..35 500) | 45 900 (41 900..49 300) | 92 µs | 54 µs |

"A replica directly" is one replica, whatever the row says: it is what one
Rayforce process does when the clients are its own. "CPU a request" is the
bouncer's processor time, user and system, over the requests of the run. It
is the steadier of the two measures on a busy machine: throughput moved by a
quarter between runs of the same thing, this by a few percent. The rows for
two and four replicas with one to a hundred clients were measured as the
machine was busiest and are the least sure; the row for two replicas and ten
clients shows no gain at all.

Requests that cost the replica about 1.6 ms,
`(count (map (fn [i] (sum (til 100000))) (til 100)))`, requests a second:

| Replicas | Clients | A replica directly | Before | After |
|---|---|---|---|---|
| 1 | 1 | 606 | 584 | 582 |
| 1 | 100 | 627 | 606 | 607 |
| 1 | 3000 | 633 | 601 | 588 |
| 2 | 100 | | 814 | 862 |
| 2 | 1000 | | 918 | 882 |
| 4 | 100 | | 1308 | 1230 |
| 4 | 1000 | | 1310 | 1284 |
| 4 | 3000 | | 1265 | 1283 |

Nothing changed here, and nothing was expected to: the time is the
replica's. Four replicas did the work of two because the machine had no
four idle cores to give them.

A connection for every request, one client, 300 requests: 112 µs a request
at the replica, 129 µs through the bouncer before and 96 µs after (median of
three; the three overlap, so read it as "the same"). More clients than one
were not measured this way: on macOS a few thousand connections a second
exhaust the machine's ports for everybody on it.

`slots`, two replicas, 1000 clients, trivial requests:

| `slots` | Requests a second | CPU a request |
|---|---|---|
| 1 | 44 500 (29 800..57 600) | 32 µs |
| 4 | 72 900 (70 300..80 000) | 42 µs |
| 16 | 79 600 (77 000..91 100) | 44 µs |

## What was changed, and what each change bought

1. **The queue is no longer read from end to end for every command.** The
   pool looked through every waiting request for clients that had left each
   time anything happened. It now looks at the request it is about to hand
   out, and through the whole queue on its 100 ms tick. Bouncer CPU a
   request, two replicas: 60 → 50 µs with 1000 clients, 106 → 70 µs with
   3000.
2. **No timer is made for each request.** The request's budget and, on
   macOS, the watch for a cancel each made and dropped timers for every
   request, under a lock all the runtime's threads share; it was 9% of the
   bouncer's busy samples in a profile. Each connection now keeps one timer
   and moves it. And **a short answer is one read and one write**, where the
   header and the body were read and written apart. Together with the next
   item: 46 → 37 µs CPU a request with one client, 53 → 42 µs with 100,
   70 → 60 µs with 1000.
3. **What a replica reports is read before what clients send.** Both came
   down one channel, so a replica that had become free waited behind every
   request sent meanwhile. See below for what this did and did not fix.

Tried and left out: running the pools' tasks on threads of their own, with a
full queue refused by the client's task. It served a refusal storm two to
six times better (below) and cost every other request a quarter more CPU.

## What limits it now

- **One request at a time for each replica connection.** A request is a full
  round trip between the bouncer and the replica, and the replica wakes for
  each one. That is about 30 µs a request here, of which the replica's share
  is 20. Where requests are trivial and there are many clients, raise
  `slots`: it moves part of the queue into the replica, which is what the
  setting's description warns of, and is the price.
- **The bouncer's work for a request**, 19 to 50 µs of CPU spread over its
  threads: one task for the client, one for the replica connection, one for
  the pool, and the channels between them.
- **On macOS, each waiting client asks its socket for a cancel 50 times a
  second.** With 1000 clients waiting on slow requests that is most of the
  bouncer's CPU (about 240 µs a request served). Linux is told by the kernel
  and pays nothing; it was not measured.
- **The backlog of connections not yet accepted** is the system's
  (`kern.ipc.somaxconn`, 128 on macOS): 3000 clients that connect in the same
  instant see some connects time out, at a replica as at the bouncer.

### Refusals

With the default `queue_max = 1000` and 3000 clients that ask again the
moment they are refused, two replicas answered 470 requests a second before
and 495 after, beside some 150 thousand refusals a second. With one replica,
`queue_max = 50` and 600 clients: 190 to 230 before, 690 to 730 after. Every
thread is busy refusing, and the tasks that serve the replicas take their
turn behind thousands of others.

This is what the reviewer's 6.8 thousand a second most likely was: 3000
clients against a queue of 1000. It is not what the bouncer does with 3000
clients (39 thousand a second above); it is what it does when two thirds of
them are refused in a tight loop. Until that is changed in the bouncer,
size `queue_max` for the clients there are, and have clients wait before
they ask again.
