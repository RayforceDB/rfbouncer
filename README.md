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
tests that run against real Rayforce processes, on Linux and macOS: the 3.0.2
release, which speaks wire version 3, and the development branch, which
speaks 4. Set `wire_version` to the one your replicas speak; the default is 4.

- On Linux a client's cancel is passed on as it arrives. Elsewhere the
  socket is asked every 20 ms, so it is up to 20 ms late.
- The statistics store keeps everything in memory, unless it is given a
  directory for the days that are over (`disk` under `[stats]`).
- TLS is there to be turned on, and is off until `[tls]` is set: without it
  the admin password and the report pages travel in the clear.

## What it does

- **Speaks Rayforce's IPC.** A client connects to the bouncer exactly as it
  would to a replica. Requests pass through byte for byte, and so do
  answers, but for what the two points below say.
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
- **Compression.** An answer is compressed for a client on another host,
  as a Rayforce server would have done had the client been its own. See
  [Compression](#compression).
- **Result cache**, off unless a pool sets `cache_ttl`: the same request
  within that time is answered from memory. See [Result cache](#result-cache).
- **Health checks.** A replica connection that has sat idle for
  `health_check_interval` (10 s) is asked something that costs nothing, and
  a replica that has not answered within `health_check_timeout` (5 s) is
  taken for hung: the connection is dropped, the replica shows as down with
  `health check: no answer within 5s` as its last error, and it is tried
  again with the usual backoff. So a replica that hangs is found while it is
  idle, not by the request that would have hung with it. The check is never
  sent on a connection that is running a request, is not counted as a
  request anywhere, and is counted in `rfbouncer_health_checks_total`. A
  request handed to a connection during a check waits for the check, its
  limit at most. A replica answers from its one evaluating thread, so the
  check cannot tell a hung replica from one busy for somebody else, a client
  of its own or another bouncer: keep the limit above the longest such work,
  or set either setting to 0 to turn the check off. With `slots` above 1
  there is no check, for the same reason.
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
- Rayforce's own client does not speak TLS, so the clients it serves stay in
  clear text; see [TLS](#tls) for what can be protected and how.
- TLS clients are not asked for a certificate: a client is known by its
  password.

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
`listen`, `admin_listen` and `[stats]` need a restart. `SIGTERM` stops taking
connections, lets the requests in flight end (for `shutdown_timeout` at
most), and exits.

When `shutdown_timeout` runs out with requests still running, each of their
replicas is sent a cancel before the process exits, and has a second to take
it: the client is answered with the error `cancel`, and no replica is left
computing an answer for nobody.

### Restarting without dropping clients

With `reuse_port = true` the port is opened with `SO_REUSEPORT`, and a second
process can open it while the first is still serving. A restart is then:

1. Start the new process, with the same configuration, beside the old one.
2. Wait for it to log `rfbouncer started`: from then on it takes connections.
3. Send the old process `SIGTERM`.

The old process closes its listening socket at once, so every connection made
from then on reaches the new one. What it had already accepted, it serves:

- A request in flight is answered in full, if it ends within
  `shutdown_timeout`. Past that it is cancelled and answered with the error
  `cancel`.
- A client that had connected and not yet sent a request, greeted or not, has
  2 s to send one, and is answered. A client that opens a connection for each
  request therefore never notices the restart.
- A persistent client is closed between two requests: after the answer to the
  request in flight, or at once if it was idle. It reads a clean end of
  stream, connects again, and reaches the new process. A request it sends
  just as its connection is closed is either read, and then answered before
  the close, or never read at all: the client then gets an end of stream or a
  reset in place of an answer, the request was run by no replica, and sending
  it again on a new connection is safe.

While both run, each has its own connections to the replicas and its own
queue, so a replica may be given two requests at once and run one after the
other; `/metrics`, the report pages and the admin console reach whichever
process the system picks. A report sums what both wrote to the statistics
store.

How the system shares the port differs. Linux spreads new connections over
all the processes listening, and a connection that has finished connecting
to the old process but that it has not yet accepted when it closes its socket
is reset; the old process accepts everything waiting before it closes, which
leaves a window of microseconds. `sysctl net.ipv4.tcp_migrate_req=1` (Linux
5.14) closes it: such connections are moved to the process still listening.
macOS and the BSDs give every new connection to the process that opened the
port last, so the old one is idle from the moment the new one starts.

`SO_REUSEPORT` lets any process of the same user open the port, and makes a
second process started by mistake serve beside the first where it would have
failed with "address in use". [`packaging/rfbouncer.service`](packaging/rfbouncer.service)
says how to do the three steps under systemd.

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

### Lockout

On top of the delay, an address that is refused `auth_lockout_failures`
times (10) within `auth_lockout_window` (1 m) is locked out for
`auth_lockout_period` (5 m): every login from it is refused unexamined, the
right password included, over IPC (the verdict a wrong password gets) and
over HTTP (`429` with `Retry-After`). `auth_lockout_failures = 0` turns it
off. Connections already open go on, and so does what needs no login:
`/healthz`, and a pool that asks its clients for nothing.

- Failures over IPC and HTTP are counted together, by IPv4 address or IPv6
  /64, and a login that succeeds does not clear them: someone who holds one
  password could otherwise guess at another without end. A client that
  speaks the wrong wire version is refused like a wrong password, and
  counted like one.
- An address is the peer of a completed TCP handshake, so it cannot be
  forged: nobody can have someone else locked out from afar.
- Clients that share an address are locked out together: behind a NAT, or
  when a proxy or a TLS terminator stands in front of the bouncer, one
  client with a wrong password locks out all of them. Raise the number or
  set it to 0 there, and limit attempts in the proxy, which sees the real
  addresses.
- At most 10 000 addresses are remembered at once. Past that a new address
  is delayed like any other and not locked out, until a window has passed.
- A lockout is logged once, with the address. `/metrics` has
  `rfbouncer_auth_lockouts_total`, `rfbouncer_auth_locked_addresses` (locked
  out now) and `rfbouncer_auth_locked_out_total` (logins refused unexamined).
  Nothing lifts one early but setting 0 and reloading, or a restart.

## TLS

```toml
[tls]
cert = "/etc/rfbouncer/fullchain.pem"   # the chain, leaf first, PEM
key = "/etc/rfbouncer/privkey.pem"      # PEM: PKCS#8, PKCS#1 or SEC1
require_for_http = true
```

With `[tls]` set the same port takes TLS as well as clear text: a TLS client
opens with a byte that neither Rayforce's IPC nor HTTP starts with, and says
which of the two it speaks inside the tunnel. So `https://host:5000/` and
IPC over TLS both work, beside what worked before. TLS 1.2 and 1.3 are
offered, with rustls and the `ring` provider, which needs a C compiler to
build and nothing else.

- `require_for_http` refuses HTTP in clear text with `403`, before any
  password is looked at. `/healthz` is the exception: it has no password and
  tells nothing, and a load balancer's check is often clear text.
- `require_for_ipc` closes an IPC connection in clear text before its
  greeting, the admin console's included. Rayforce's own client does not
  speak TLS, so leave this off unless every client comes through TLS: a
  client library that has it, or a tunnel such as `stunnel` on the client's
  machine.
- Both are counted in `rfbouncer_cleartext_refused_total`.
- `SIGHUP` reads the two files again with the rest of the configuration.
  Connections already open keep the certificate they were made under. A
  certificate or key that cannot be read, or that do not belong together,
  fails `--check` and a reload alike, and the old one stays.
- Relative paths are taken from the directory the bouncer was started in.

**Cancel over TLS.** A client cancels a request with one byte of TCP urgent
data, which TCP carries beside the stream. TLS has no place for it, so over
TLS the bouncer looks for that byte where it always did: on the TCP
connection, beneath the tunnel. A client that can reach its socket sends it
there and is cancelled as in clear text. That byte is not protected by TLS:
whoever can inject into the connection can cancel a request, as they could
by resetting the connection. A client that cannot reach its socket, or that
comes through something that ends the TCP connection on the way (a TLS
terminator, a tunnel), cancels by disconnecting, which always works.

## An address of its own for administration

```toml
listen = "0.0.0.0:5000"
admin_listen = "127.0.0.1:5050"
```

With `admin_listen` set, the admin console, the report pages and `/metrics`
are served there and nowhere else, and that address serves no pool. `listen`
then takes pool clients only: the user `rfbouncer` is refused there like a
wrong password, and every HTTP path but `/healthz` answers `404`.

- `/healthz` is on both: a load balancer checks the port it sends clients
  to, and a monitor may sit on the other side.
- The admin address asks every IPC client for credentials, so the console is
  there even when the one pool asks its clients for nothing.
- `[tls]`, the lockout and `max_client_conn` apply to each address alike.

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
| `flush [pool]` | empty the result cache |
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

The wait for a replica has percentiles like the time in one: p50, p95 and
p99 in the totals, in the tooltip of the wait chart, and p95 for each query
shape.

Beside each total stands how it moved against the period before, the equally
long one that ends where the selected range begins: `▲ 34%`, `▼ 12%`,
`▲ 3.2×`, and the earlier number on hover. Queries shows the same for each
shape's requests and average exec, and `new` for a shape the period before
did not have. Where a number moved by less than a tenth nothing is shown:
rows are whole minutes and percentiles are bucket edges, so the two periods
are alike only to about that.

The page of one query shape also lists the clients that send it and the
replicas that run it. A minute keeps at most `max_fingerprints_per_minute`
pairs of a shape and a client, and as many of a shape and a replica; past
that a shape's further clients are counted together as `<other>`.

### Symbols in query shapes

Symbols are kept in a shape, which is right while they name tables and
columns and wrong for a workload that asks for thousands of instruments by
name: every one of them is a shape of its own. `normalize_symbols = true`
under `[stats]` replaces them with `?` as other values are: `'name` (and a
character, `'c'`), `:name`, and the bare names in a `[ ]` vector, which
Rayforce reads as symbols too. So `(== sym 'AAPL)` and `(== sym 'MSFT)` are
both `(== sym ?)`, and `(in sym [AAPL MSFT])` is `(in sym [?])`.

What stays is what its place shows to be part of the query rather than a
value in it:

- the table after `from:`, as in `{from: 'trades}`. Queries of different
  tables are different queries with different costs, and there are only as
  many tables as the replicas hold;
- the columns after `by:`, `asc:` and `desc:`, as in `by: [sym side]`;
- a function's parameters, `(fn [x y] ...)`.

Everything else goes, column names and type names included: `(at t 'price)`
is `(at t ?)` and `(as 'I64 x)` is `(as ? x)`, since the text cannot tell
them from a value. Turning the setting on or off gives every affected query
a new shape from then on; what was counted before stays under the old one.

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
| `rfb_agg` | a minute, a pool and a query shape: counts by outcome (`cached` among them), time and wait sums and maxima, bytes, a histogram of time in `h0`..`h31` (`h<i>` counts requests under 2^i µs) and one of the wait in `w0`..`w31` |
| `rfb_status` | a minute, a pool and an outcome (`ok`, `cached`, `cancel`, `timeout`, `busy`, `io`, or a replica's error code) |
| `rfb_backend`, `rfb_client` | a minute, a pool and a replica or a client address: requests, those that failed, those answered from the cache, and time |
| `rfb_fp` | a query shape's text |
| `rfb_sample` | a request kept whole: the slowest of each hour and up to 20 failed a minute |
| `rfb_fp_client`, `rfb_fp_backend` | a minute, a pool, a query shape and a client address or a replica |

A store whose tables are of an earlier layout is refused with a message that
names the table and the column it lacks: start an empty store, or delete the
`rfb_` tables in the old one.

Several bouncers may share one store: every row carries the process that
wrote it (`src`), and a report sums them. A bouncer that restarts writes rows
of its own from then on, so nothing it counted before is lost or doubled.

While the store is away, requests are served as before, the reports answer
`502`, and the minute being filled waits in memory; a store that stays away
past the end of a minute costs that minute.

Sizing. Everything the store holds is in memory (of the current day only,
with `disk` set: see [Days on disk](#days-on-disk)), about 650 bytes a row of
`rfb_agg`, one row a minute for each query shape seen in it. Names and
symbols are part of a shape, so `(== sym 'AAPL)` and `(== sym 'MSFT)` are two:
a workload with many distinct symbols has many shapes, unless
`normalize_symbols` is set. `rfb_fp_client` and `rfb_fp_backend` hold nine
numbers a row and at most twice `max_fingerprints_per_minute` rows a minute
each. The journal grows with
every row written, and only rows that changed are written. Lower
`retention_aggregates` or `max_fingerprints_per_minute` if either is too
much.

### Days on disk

With `disk = "/var/lib/rfbouncer/days"` under `[stats]` the store keeps only
the current day in memory. The path is on the store's host, and the store
writes there, not the bouncer. An hour after a day ended (UTC), the bouncer
has the store write that day's rows to `<disk>/<day>/<table>`, a splayed
table for each of `rfb_agg`, `rfb_status`, `rfb_backend`, `rfb_client`,
`rfb_fp_client` and `rfb_fp_backend`, and drop them from memory. `<day>` is
the number of days since 1970. It is the layout Rayforce reads as one table
partitioned by day:

```lisp
(select {from: (.db.parted.get "/var/lib/rfbouncer/days" 'rfb_agg)
         where: (>= part 20000) by: part n: (sum count)})
```

A report whose range reaches back to a day on disk asks both and adds the
answers up; one of today asks memory alone. Days are moved when a bouncer
connects to the store and every hour after; with several bouncers on one
store, whichever asks first moves a day and the others find it done.

What to know before turning it on:

- **Nothing on disk is ever deleted.** Rayforce has no function that removes
  a file, so `retention_aggregates` applies to memory only. Remove the
  directories of days you no longer want, oldest first, with a cron job.
- `rfb_fp` and `rfb_sample` stay in memory: they are small, and a sample is
  looked up by its time, not its day.
- A day on disk is final. Rows of it that reach the store later, from a
  bouncer that could not reach the store for over an hour around midnight,
  are dropped.
- A day is written in one message to the store, which answers nothing else
  meanwhile. A bouncer gives the store ten seconds to answer and logs an
  error past that; a day that was not finished is written again the next
  hour.
- If the store dies while writing a day and has no journal to bring the day
  back from, the day's directory is left without `rfb_agg` and no day on
  disk can be read until that directory is removed. The bouncer logs
  `the days under ... cannot be read`.

## Compression

A Rayforce server compresses an answer of more than 2000 bytes for a client
on another host and sends it plain to one on its own. Behind the bouncer a
replica's peer is the bouncer, which mostly sits on the replica's host, so a
client far away would get every answer plain. The bouncer does what the
replica would have done: the same scheme, giving the same bytes, for a client
whose address is not loopback (`127.0.0.0/8`, `::1`).

| Setting, per pool | |
|---|---|
| `compress` | `"remote"` (the default): clients on another host. `"always"`: every client. `"off"`: nobody |
| `compress_max_bytes` | the largest answer compressed, `16MiB` unless set |

- Compressing needs the whole answer in memory, where otherwise it is passed
  on as it comes, 64 KiB at a time. While an answer is compressed a replica
  connection holds up to twice `compress_max_bytes`. A larger answer is passed
  on plain, as before.
- An answer that does not get smaller is sent plain, as a server does. One a
  replica compressed itself, being on another host, is passed on as it is.
- The budgets are what they were: `query_timeout` until the replica begins
  to answer, `transfer_timeout` from then until the client has it all, which
  now includes taking it from the replica and compressing it. Nothing
  reaches the client until that is done.
- If the replica's connection breaks in the middle of such an answer,
  nothing of it has reached the client, and the request is run again as
  `retry` says. An answer passed on plain is half sent by then, and its
  client's connection is closed.

What it buys depends on the data: Rayforce's scheme is run lengths of byte
differences, which finds the zero bytes of small integers and little else.
Measured on one core of an Apple M4, on results as a replica serialized them:

| Result | Plain | Compressed | Saved | Time | |
|---|---|---|---|---|---|
| table of 100 000 rows: a running id, a float, a small integer | 2.40 MB | 2.02 MB | 15.7 % | 4.2 ms | 545 MiB/s |
| table of 50 000 rows: two symbols, a string, a timestamp | 1.84 MB | 1.70 MB | 7.4 % | 2.4 ms | 728 MiB/s |
| 1 000 000 integers counting up | 8.00 MB | 6.93 MB | 13.3 % | 9.8 ms | 781 MiB/s |
| 50 000 short strings | 0.94 MB | 0.80 MB | 15.3 % | 1.3 ms | 702 MiB/s |
| 1 000 000 random floats | 8.00 MB | not smaller, sent plain | 0 % | 22.9 ms | 334 MiB/s |

A tenth of the bytes for a few milliseconds of one core per megabyte-sized
answer: worth it on a slow link, not on a fast one. Set `compress = "off"`
where the network is not what limits you. `/metrics` counts the answers
compressed (`rfbouncer_compressed_total`) and the bytes that saved
(`rfbouncer_compress_saved_bytes_total`). The numbers above come from
`cargo test --release --test compress -- --ignored --nocapture`.

## Result cache

With `cache_ttl` set, a pool keeps the answers its replicas give and hands
one back to a request made of the same bytes, without asking a replica. It is
off unless set.

| Setting, per pool | |
|---|---|
| `cache_ttl` | how long an answer may be given again; `0`, the default, keeps nothing |
| `cache_max_bytes` | the most memory the kept answers and their requests may take, `64MiB` unless set; the oldest go first |
| `cache_max_entry` | the largest answer kept, counted with its request, `1MiB` unless set |

What it means for the answers a client gets:

- **An answer may be `cache_ttl` old.** Replicas change underneath; the
  bouncer does not know when. The age is counted from the moment the request
  was handed to its replica, so what a client reads is never older than
  `cache_ttl`, and the setting is the staleness you accept. `flush [pool]` on
  the admin console empties the cache at once, for when the replicas were
  just given new data; an answer on its way from a replica at that moment is
  not kept either. Changing a pool's settings or its replicas empties it too.
- **The same bytes, the same answer.** The key is the whole request frame,
  so `(+ 1 2)` and `(+ 1  2)` are two requests. The bouncer does not read a
  request to guess whether it may be kept: one whose answer depends on the
  time or on chance, such as `(now)` or `(rand 10 100)`, gets the same answer
  for `cache_ttl`. Send such requests through a pool without a cache: a
  second pool over the same replicas, picked by another user name, costs
  nothing.
- **It is for reads.** A request that changes something on a replica that
  lets it (one without `-U`, or a function marked with `.sys.trust`) is not
  run again while its answer is kept.
- **Only whole, good answers are kept.** An error a replica raised, a
  request that timed out, was cancelled, was turned away as `busy` or lost
  its replica leaves nothing behind: the next one like it goes to a replica.
- **Any client's answer.** A pool's clients share its cache, whichever
  replica answered; `affinity` does not hold for an answer from memory.
  Pools do not share: each has its own. Clients that are sent compressed
  answers and clients that are not are kept apart, so each gets the bytes it
  would have got from a replica.
- **The same request twice at once goes to a replica twice.** While the
  first is still running there is nothing to give the second, and it is not
  made to wait for it: tied to another client's request it would inherit that
  client's cancel, that request's timeout and its replica's failure, and
  would then have to start over, later than if it had not waited. With a
  replica free it loses nothing by running; the cost is that a burst of the
  same slow request is run once per free replica. A request that waits in
  the queue anyway, every replica being busy, is looked up again when its
  turn comes, and is given the answer its twin left meanwhile.

An answer from memory needs no replica and no place in the queue: it is given
while every replica is busy and when the queue is full. A paused pool holds
it like any other request.

In the statistics such a request has the outcome `cached`, no replica and no
time in one. It is not counted as failed and has a count of its own (`cached` in
`rfb_agg` and beside each client), which the pages show with the requests
and among the outcomes. It is left out of the times, the waits and both
histograms, so the percentiles and the averages are those of the requests
that asked for a replica: an average divides by them, not by all. That goes
for the wait of a request that stood in the queue before its twin's answer
was there for it, too.
`/metrics` has `rfbouncer_requests_total{outcome="cached"}`,
`rfbouncer_cache_hits_total`, `rfbouncer_cache_misses_total` (requests handed
to a replica while the cache is on; one that is run again after its replica
failed counts twice), `rfbouncer_cache_entries` and `rfbouncer_cache_bytes`.

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
