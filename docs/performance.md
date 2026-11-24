# Performance

How fast the bouncer is under many clients, how that was measured, and what
limits it. The short of it:

- A replica asked by one client at a time answers about 50 thousand trivial
  requests a second on the machine below; asked by ten or more clients at
  once, about 100 thousand, because it then reads several requests for each
  time it wakes.
- The bouncer gives a replica connection one request at a time, so each
  connection is the first case, plus the bouncer's own turn-around: 24 to 38
  thousand trivial requests a second for one replica, 35 to 41 thousand for
  two, 44 to 64 thousand for four.
- `slots = 4` gives a replica four connections and so the second case: two
  replicas then answer 71 thousand trivial requests a second, and 77 thousand
  with `slots = 16`.
- For requests that cost the replica a millisecond or more the bouncer is not
  what limits anything: it costs about 4% on one replica.
- Clients that are refused with `busy` and ask again at once used to bring
  the bouncer down to a few hundred requests a second. `busy_delay` holds
  each refusal back for 100 ms, and the same load is now served at 33
  thousand. See [Refusals](#refusals).

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
wire version 4), the bouncer and `rfbench`. The machine was doing other work
too, with a load average between 3 and 6, so every number is the median of
three runs of two seconds, with the least and the most beside it where they
overlap, and "before" and "after" were run in turns within the same minute.
The pool had `queue_max = 100000`. "Before" is commit bef9b7c, which has
TLS, the result cache, compression, health checks, the status streams and
the wait histogram, and none of what this page describes.

Trivial requests, `(+ 1 2)`, persistent connections, requests a second:

| Replicas | Clients | A replica directly | Before | After | CPU a request, before | after |
|---|---|---|---|---|---|---|
| 1 | 1 | 51 700 | 23 600 | 24 300 | 22 µs | 18 µs |
| 1 | 10 | 130 800 | 38 500 | 38 400 | 25 µs | 23 µs |
| 1 | 100 | 112 300 | 34 100 | 36 200 | 30 µs | 25 µs |
| 1 | 1000 | 102 800 | 28 600 | 32 900 | 40 µs | 31 µs |
| 1 | 3000 | 95 100 | 21 000 | 28 500 | 80 µs | 53 µs |
| 2 | 1 | | 23 100 | 24 000 | 23 µs | 19 µs |
| 2 | 10 | | 46 600 (37 900..56 800) | 40 200 (39 800..58 700) | 32 µs | 33 µs |
| 2 | 100 | | 40 500 | 41 200 | 40 µs | 32 µs |
| 2 | 1000 | | 32 500 | 36 800 | 55 µs | 41 µs |
| 2 | 3000 | | 26 200 | 34 700 | 95 µs | 57 µs |
| 4 | 1 | | 22 800 | 23 800 | 23 µs | 19 µs |
| 4 | 10 | | 67 000 (54 400..67 800) | 63 600 (57 300..74 500) | 37 µs | 34 µs |
| 4 | 100 | | 50 100 | 53 800 | 53 µs | 41 µs |
| 4 | 1000 | | 43 600 | 50 000 | 65 µs | 47 µs |
| 4 | 3000 | | 32 600 | 44 200 | 98 µs | 61 µs |

"A replica directly" is one replica, whatever the row says: it is what one
Rayforce process does when the clients are its own. "CPU a request" is the
bouncer's processor time, user and system, over the requests of the run. It
is the steadier of the two measures on a machine that is not idle. With ten
clients nothing was gained: the runs overlap, and so does the CPU.

Requests that cost the replica about 1.6 ms,
`(count (map (fn [i] (sum (til 100000))) (til 100)))`, requests a second:

| Replicas | Clients | A replica directly | Before | After |
|---|---|---|---|---|
| 1 | 1 | 583 | 558 | 556 |
| 1 | 100 | 615 | 591 | 602 |
| 1 | 3000 | 632 | 572 | 581 |
| 2 | 100 | | 1034 | 1038 |
| 2 | 1000 | | 905 | 908 |
| 2 | 3000 | | 901 | 906 |
| 4 | 100 | | 1471 | 1464 |
| 4 | 1000 | | 1223 | 1244 |
| 4 | 3000 | | 1230 | 1270 |

Nothing changed here, and nothing was expected to: the time is the
replica's. The two rows with 100 clients are from a second run of five
rounds with "after" run first. In the first, where it ran second, it was 8%
behind (858 against 936, 1087 against 1188), which the second run did not
show again: with replicas that fill their cores, which run comes first
matters more than which bouncer it is. Two and four replicas do not do two
and four times the work of one because the machine had no idle cores to
give them.

A connection for every request, one client, 300 requests: 116 µs a request
at the replica, 105 µs through the bouncer before and 108 µs after, the same.
More clients than one were not measured this way: on macOS a few thousand
connections a second exhaust the machine's ports for everybody on it.

`slots`, two replicas, 1000 clients, trivial requests:

| `slots` | Before | After | CPU a request, before | after |
|---|---|---|---|---|
| 1 | 40 300 | 47 400 | 45 µs | 32 µs |
| 4 | 61 000 | 70 500 | 62 µs | 44 µs |
| 16 | 66 100 | 76 900 | 67 µs | 48 µs |

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
   request sent meanwhile.
4. **A client's stream is not split into a reading and a writing half**,
   whose every use takes a lock they share, and **the status board is not
   shown the replicas for what a client sends**, which changes nothing about
   them. Small: 18.6 → 18.4 µs CPU a request with one client; with more
   clients the difference was within the noise.
5. **A request that finds the queue full is told so after `busy_delay`.**
   See [Refusals](#refusals).

The figures in 1 and 2 are from before the features named above were merged,
against the commit before them; they were not measured apart again. Those
features cost a request about 1 µs of CPU with `slots = 1` (18.1 → 18.8 µs
with one client, 24.0 → 25.2 µs with 1000) and 3 µs with `slots = 16`, where
the status board reads 32 connections each time a request ends. A cache that
is off costs a comparison, and the wait histogram one addition.

Tried and left out: running the pools' tasks on threads of their own, with a
full queue refused by the client's task. It served a refusal storm two to
six times better and cost every other request a quarter more CPU; the delay
serves it seventy times better and costs the others nothing.

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

A client that finds the queue full is answered `busy`. Told at once, a
client that asks again the moment it is refused does so tens of thousands of
times a second, every thread is busy refusing, and the tasks that serve the
replicas take their turn behind thousands of others. So the answer is held
back for `busy_delay`, 100 ms unless set: a refused client then asks at most
ten times a second, whatever it does. Requests a second, with clients that
do not wait before they ask again:

| Load | Before | After | With `busy_delay = "0"` |
|---|---|---|---|
| 2 replicas, 3000 clients, `queue_max = 1000` | 469, and 141 000 refusals | 33 400, and 20 000 refusals | 484 |
| 1 replica, 600 clients, `queue_max = 50` | 333, and 160 000 refusals | 31 200, and 5 500 refusals | |

The same two replicas with 3000 clients and room in the queue for all of
them answer 34 700 a second, so a full queue now costs next to nothing. A
client sees the difference in one place: its `busy` comes 100 ms later. A
request turned away for having waited past `query_wait_timeout` is told at
once, as before: it has waited already.

This is what the reviewer's 6.8 thousand a second most likely was: 3000
clients against a queue of 1000.
