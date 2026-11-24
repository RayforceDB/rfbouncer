# rfbouncer

One port in front of a pool of read-only [Rayforce](https://github.com/RayforceDB/rayforce)
replicas, with request statistics kept in Rayforce and a report page on the
same port.

A Rayforce process evaluates one message at a time, so one slow query holds
every other client of that process, and replicas cannot share a port.
rfbouncer keeps the queue itself and hands each replica one request at a
time: a short query never waits behind a long one while another replica is
free.

```
clients ──► :5000 rfbouncer ──► replica :5001
browser ──►   │   queue      ──► replica :5002
              │              ──► replica :5003
              └─ statistics ──► a Rayforce of their own :5100
```

## Status

Version 0.1: early, and not yet run in production. What it does is covered by
tests against real Rayforce processes, on Linux and macOS, at wire version 4.

- Wire version 3 is supported by the layout of its bytes and has not been
  run against a version 3 server.
- On Linux a client's cancel is passed on as it arrives. Elsewhere the
  socket is asked every 20 ms, so it is up to 20 ms late.
- The statistics store keeps everything in memory.
- There is no TLS: the admin password and the report pages travel in the
  clear.

## What it does

- **Speaks Rayforce's IPC.** A client connects to the bouncer exactly as it
  would to a replica. Requests and answers pass through byte for byte.
- **One request, one replica.** Each sync request goes to a free replica,
  round robin; with every replica busy it waits in the bouncer's queue.
- **Two ways to choose a replica**, set per pool with `mode`. `free`, the
  default, is the above. `round_robin` gives each request the next replica in
  turn whether it is free or not: every replica gets the same number of
  requests, and a request may wait behind a slow one. Either way a replica
  that is down gets nothing.
- **Cancel.** A client's cancel byte takes its request out of the queue, or
  is passed on to the replica running it. A client that disconnects cancels
  its request the same way, so a replica never finishes a query for nobody.
- **Budgets.** A request whose replica has not begun to answer within
  `query_timeout` (30 s unless set) is cancelled and answered with the error
  `timeout`; one that waits for a replica past `query_wait_timeout`, or finds
  the queue full, is answered with `busy`. A client that takes longer than
  `transfer_timeout` to receive an answer is cut off, so a slow reader cannot
  hold a replica.
- **Failing replicas.** A replica that dies before any of its answer reached
  the client has the request run again, once, on another replica if there is
  one; the wait for that replica is counted afresh. A dead replica is retried
  with a backoff and used again once it answers a greeting.
- **Pools.** Several pools on one port; the user name in a client's
  credentials picks the pool.
- **Admin console**, read with any Rayforce client.
- **Statistics and reports**: requests grouped by the shape of their query,
  kept in a Rayforce process of their own, shown on the bouncer's port over
  HTTP.

## What it does not do

It is for reads. Send writes, transactions and subscriptions straight to the
process that owns them.

- An async message (one that expects no answer) closes the connection.
- A transaction cannot be opened through it: replicas run with `-U`.
- A subscription made through it is dropped: a replica that pushes on a
  pooled connection has that connection replaced.
- `.ipc.on.*` hooks on a replica see the bouncer, not the client.
- Connections stay on TCP; the shared-memory and RDMA transports are not
  offered.
- A client must keep its connection whole until it has its answer. One that
  closes its sending side after the request is taken to have left, and its
  request is cancelled: the two cannot be told apart, and a client that left
  must not keep a replica busy.
- No TLS. Keep the port on a private network, or put a proxy in front.

## Running it

```sh
cargo build --release
./target/release/rfbouncer --config rfbouncer.toml
./target/release/rfbouncer --config rfbouncer.toml --check   # validate and exit
./target/release/rfbouncer --config rfbouncer.toml --log-level debug
```

Start each replica restricted, so it refuses writes:

```sh
rayforce -p 5001 -U replica-secret
```

[`rfbouncer.example.toml`](rfbouncer.example.toml) lists every setting with
its meaning and default. The smallest configuration:

```toml
listen = "0.0.0.0:5000"

[[pool]]
name = "main"
backend_password = "replica-secret"
backends = ["127.0.0.1:5001", "127.0.0.1:5002"]
```

`SIGHUP` reads the configuration again: limits, timeouts and passwords apply
at once, replicas added are connected, replicas removed finish their request
and go. A file that does not parse leaves the running configuration alone.
`listen` and `[stats]` need a restart. `SIGTERM` stops taking
connections, lets the requests in flight end (for `shutdown_timeout` at
most), and exits.

### Give each replica its own cores

A Rayforce process runs a query's parallel parts on a worker pool sized to the
machine. N replicas each sized to the whole machine fight over the same
cores. Start each with `-c` so that together they fit, or pin them with a
cpuset.

## Credentials

| Situation | What a client presents |
|---|---|
| One pool, no `password` | nothing: clients are not asked |
| One pool with `password` | any user name and that password |
| Several pools | a user name listed in a pool's `users`, and that pool's password if it has one; an unknown name lands in the `default` pool |
| Admin console | user `rfbouncer` and `admin_password` |
| Report pages, `/metrics` | `report_password` or `admin_password`, over HTTP Basic |

The admin console is reached through the credentials, so it needs clients to
be asked for them: with one pool, give the pool a `password`.

A refused login is logged with its address, counted in
`rfbouncer_auth_failures_total`, and answered half a second late. A client
that speaks another wire version than its pool is refused the same way. A
changed password applies to new connections; the ones already open stay.

## Admin console

```lisp
(set h (.ipc.open "127.0.0.1:5000:rfbouncer:admin-secret"))
(show (.ipc.send h "show servers"))
```

| Command | |
|---|---|
| `show pools` | queue length, replicas up and busy, counters, average wait and time |
| `show servers` | each replica: state, what it is running and for whom, last error |
| `show clients` | who is connected |
| `show stats`, `show config` | the process, and the settings in force (no passwords) |
| `pause [pool]`, `resume [pool]` | hold requests in the queue, for as long as it takes; let them go |
| `drain <addr>`, `undrain <addr>` | stop handing a replica requests; start again |
| `reconnect [pool]` | open every replica connection anew |
| `reload`, `shutdown` | as `SIGHUP` and `SIGTERM` |

## Reports

Open `http://host:5000/` in a browser and give `admin_password` (any user
name). With `[stats] rayforce` set, the pages show:

- **Overview**: requests, time in the replica (p50, p95, p99), requests that
  did not succeed, and the wait for a free replica, over time.
- **Queries**: requests grouped by the shape of their query, heaviest first.
  `(+ 1 2)` and `(+ 3 4)` are both `(+ ? ?)`; names and symbols are kept, as
  they are mostly tables and columns.
- **Slowest**, **Failures**: individual requests kept with their full text.
- **Clients**, **Replicas**: who sends the work and who does it.
- **Now**: what each replica is running and what waits in the queue.

Only requests sent as source text have a text to show; a request sent as a
serialized value is told from others by its bytes and shown as
`<binary N bytes>`. Percentiles come from a histogram
whose buckets double, so each is the upper edge of its bucket.

The slowest and the failed requests are stored with their full text, values
included. Set `store_samples = false` if those must not be kept.

`/metrics` serves counters in the Prometheus format, behind the same
passwords: requests by outcome, histograms of time in the replica and of the
wait for one, queue length, each replica's state, refused logins and
connections.

`/healthz` needs no password. It answers `200` while at least one replica is
connected and `503` otherwise, and says nothing else: point a load balancer's
check at it.

`/api/live` and the admin's `show servers` show the text of what is running
whatever `store_samples` says: that setting is about what is kept.

### Where the statistics live

In a Rayforce process of their own, named under `[stats]`:

```sh
rayforce -p 5100 -u stats-secret -L /var/lib/rfbouncer/stats
```

It is not one of a pool's replicas and it takes writes, so it runs with `-u`
rather than `-U`. The journal (`-L`) is what makes it survive a restart;
without one it starts empty. The bouncer makes its tables there and upserts
the minute being filled every ten seconds. They are ordinary tables:

```lisp
(set h (.ipc.open "127.0.0.1:5100:me:stats-secret"))
(.ipc.send h "(select {from: rfb_agg by: pool n: (sum count)})")
```

| Table | A row is |
|---|---|
| `rfb_agg` | a minute, a pool and a query shape: counts by outcome, time and wait sums and maxima, bytes, and a histogram of time in `h0`..`h31` (`h<i>` counts requests under 2^i µs) |
| `rfb_status` | a minute, a pool and an outcome (`ok`, `cancel`, `timeout`, `busy`, `io`, or a replica's error code) |
| `rfb_backend`, `rfb_client` | a minute, a pool and a replica or a client address |
| `rfb_fp` | a query shape's text |
| `rfb_sample` | a request kept whole: the slowest of each hour and up to 20 failed a minute |

Several bouncers may share one store: every row carries the process that
wrote it (`src`), and a report sums them. A bouncer that restarts writes rows
of its own from then on, so nothing it counted before is lost or doubled.

While the store is away, requests are served as before, the reports answer
`502`, and the minute being filled waits in memory; a store that stays away
past the end of a minute costs that minute.

Sizing. Everything the store holds is in memory, about 400 bytes a row of
`rfb_agg`, one row a minute for each query shape seen in it. Names and
symbols are part of a shape, so `(== sym 'AAPL)` and `(== sym 'MSFT)` are two:
a workload with many distinct symbols has many shapes. The journal grows with
every row written, and only rows that changed are written. Lower
`retention_aggregates` or `max_fingerprints_per_minute` if either is too
much.

## Development

```sh
cargo test          # needs a Rayforce binary
```

The integration tests start real Rayforce processes. They take the binary
from `RAYFORCE_BIN`, or from `.cache/rayforce` beside `Cargo.toml`.

The wire is written against Rayforce's `src/core/ipc.c` and
`src/store/serde.c`; the expected bytes in the unit tests were read off a
running server. Recheck `src/wire/` when Rayforce's wire version moves.

## License

MIT. See [LICENSE](LICENSE).
