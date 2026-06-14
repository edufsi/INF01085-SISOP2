# Current Repository Assessment for Part 2

> Historical note: this assessment describes the repository before the Part 2
> implementation was added. See `README.md` for the current architecture and
> operating instructions.

## Executive summary

The repository is a small, understandable implementation of the Part 1
reliable-summation service. It already provides a useful application protocol,
client discovery, per-client request sequencing, retransmission, duplicate
suppression, and in-memory aggregate state.

It is a good **behavioral prototype** for Part 2, but it is not a directly
compliant implementation foundation for the supplied Part 2 handout. The two
documents in the repository belong to different offerings:

| Document | Course and semester | Required language | Server concurrency |
| --- | --- | --- | --- |
| `INF01151-Trabalho_202601_P1.pdf` | INF01085, 2026/1 | Python 3 | Not required |
| `INF01151-Trabalho_202501_P2.pdf` | INF01151, 2025/1 | C/C++ | One thread per request |

The current code follows the 2026/1 Part 1 handout: it is written in Python and
the server is deliberately single-threaded. The supplied 2025/1 Part 2 handout
requires C/C++ and concurrent request processing. If that handout is truly the
target specification, most code must be rewritten, although the protocol and
state model remain useful design references.

No replication, failure detection, leader election, or leader-change
notification exists yet.

## Repository map

The implementation contains about 440 lines of Python split into client and
server modules:

- `client/discovery.py`: UDP broadcast discovery.
- `client/processing.py`: stop-and-wait request and ACK protocol.
- `client/interface.py`: input/output threads, queues, timeout calculation, and
  the client control loop.
- `client/main.py`: command-line entry point.
- `server/state.py`: aggregate state and per-client state.
- `server/processing.py`: discovery and request handlers.
- `server/interface.py`: UDP socket setup and the single-threaded receive loop.
- `server/main.py`: command-line entry point.
- `README.md` and `Relatorio.pdf`: Part 1 design report and usage instructions.

There is no test suite, Makefile, dependency manifest, automated benchmark, or
machine-readable protocol definition.

## Current architecture

### Discovery

The client broadcasts `DESCOBERTA` to the configured UDP port. The first server
that replies with `IP_SERVIDOR_OK` is selected, and the client retains the
sender's IP address.

This implements the Part 1 discovery flow, but it has no concept of replica
identity, role, term/epoch, priority, or authoritative leader. If several
replicas answer discovery, the client simply accepts whichever response arrives
first.

### Request processing

Requests use this text format:

```text
<request_id>,<integer_value>
```

Successful responses use:

```text
ACK,<request_id>,<global_request_count>,<global_sum>
```

Each client sends only one outstanding request. On timeout it retransmits the
same request. The server tracks the last accepted request ID for each
`(source IP, source port)` pair:

- the expected ID is applied exactly once;
- an older ID is treated as a duplicate and receives the previous ACK state;
- a future ID is not applied and receives an ACK for the last accepted ID.

This is the strongest reusable part of the repository. It provides a clear
idempotency key and enough state for backups to preserve exactly-once behavior
after failover, provided the complete per-client table is replicated.

### State

`ServerState` contains:

- total number of accepted requests;
- global accumulated sum;
- a map of client addresses to `ClientState`.

Each `ClientState` contains:

- last accepted request ID;
- global request count returned for that request;
- global sum returned for that request.

Replicating only the global sum would be insufficient. The complete client map
and its saved ACK values are required so that a promoted backup can correctly
answer a retransmission whose ACK was lost during primary failure.

### Concurrency

The client uses:

- one output thread;
- one daemon input thread;
- a main network-processing thread;
- thread-safe queues between them.

The server has one receive loop and no worker threads or locks. This makes the
current state updates internally simple and deterministic, but directly
conflicts with the supplied Part 2 requirement to process each request in a
thread.

## Verified behavior

The following checks were run against the current branch:

- all Python modules compile successfully with `python3 -m compileall`;
- broadcast discovery finds the local server;
- a normal request updates the aggregate and returns the expected ACK;
- a duplicate request is not added twice;
- an out-of-order future request is rejected without changing the aggregate;
- two clients, distinguished by source port, maintain independent sequences;
- the interactive client and server work together end to end;
- after server restart, the client receives `RESET`, resets its sequence, and
  continues against a new empty aggregate.

The restart test also confirms an important Part 2 incompatibility: the old
aggregate is lost. The observed aggregate changed from 26 before restart to 6
after restart. Passive replication must remove the need for this destructive
reset path.

## Requirement coverage

| Part 2 requirement | Current status | Assessment |
| --- | --- | --- |
| Distributed integer-sum service | Implemented | Core behavior exists. |
| UDP sockets on Unix/Linux | Implemented | Uses IPv4 UDP sockets and works on Linux. |
| C/C++ implementation | Not implemented | Current code is Python 3. |
| Positive integer input | Partial | Integers are parsed, but zero and negatives are accepted. |
| Multiple clients | Implemented | Clients are keyed by IP and source port. |
| Exactly-once summation over UDP | Largely implemented | Stop-and-wait and duplicate suppression cover Part 1 failures. |
| One thread per server request | Not implemented | Server is single-threaded. |
| Correct shared-state synchronization | Not implemented | Locks will be required after introducing workers and replication. |
| Primary replica manager | Not implemented | No roles or replica configuration exist. |
| One or more backup RMs | Not implemented | No server-to-server protocol exists. |
| Propagate state after every sum | Not implemented | No replication messages or acknowledgements exist. |
| All clients use the same primary | Not guaranteed | Discovery accepts the first responder. |
| Detect primary failure | Not implemented | Client retries forever at the old address. |
| Bully leader election | Not implemented | No IDs, priorities, heartbeats, or election messages exist. |
| Promote a consistent backup | Not implemented | No replicated state or promotion logic exists. |
| Notify clients of the new leader | Not implemented | The selected server IP is fixed after discovery. |
| 10,000,000+ request performance | Unverified and unlikely | No benchmark; stop-and-wait and per-request printing are major limits. |
| Automated compilation | Not implemented | No Makefile or build script exists. |
| Automated tests | Not implemented | Validation is currently manual. |

## Strengths as a foundation

1. **Clear module boundaries.** Discovery, request processing, interface code,
   and server state are already separated. Equivalent boundaries can be kept in
   a C/C++ rewrite.
2. **Useful idempotency model.** Per-client request IDs and saved ACK snapshots
   are exactly the state a failover-capable primary needs.
3. **Correct duplicate behavior in normal operation.** Retransmissions do not
   change the aggregate more than once.
4. **Simple wire protocol.** The current messages are easy to inspect and can
   be extended with replica and election message types.
5. **Client identity supports local testing.** Using IP plus source port allows
   multiple clients on one machine, unlike an IP-only key.
6. **Existing discovery mechanism.** Broadcast discovery can become the initial
   leader lookup mechanism once only the elected primary is allowed to answer.

## Risks and design gaps

### Specification mismatch

The largest risk is proceeding before confirming which Part 2 specification
applies. The repository and Part 2 PDF do not belong to the same course/year.
If the 2026 continuation still permits Python or changes the concurrency
requirements, a C/C++ rewrite based on the 2025 handout could be wasted work.

### Failover correctness

The current `RESET` behavior intentionally discards continuity after a server
restart. In Part 2, a client may retransmit a request after the primary applied
and replicated it but failed before returning the ACK. The new primary must
find that request in the replicated client table and return the previous ACK,
not reset the client or add the value again.

### Replication ordering

Concurrent worker threads can complete in a different order on primary and
backups. State updates need a single authoritative sequence number or log index.
Backups must apply updates in that order. Sending independent snapshots from
workers without ordering can regress a backup to an older state.

### Meaning of "after each operation"

The handout says the primary propagates state after each sum, but does not state
whether the client may be acknowledged before backups confirm. A safer passive
replication design is:

1. lock and validate the client request;
2. assign a monotonically increasing state version;
3. apply the operation on the primary;
4. replicate the resulting state/update to backups;
5. wait for the chosen acknowledgement policy;
6. reply to the client.

The team should document whether it waits for all backups or a quorum and what
happens when a backup is unavailable.

### Split brain and stale messages

The current protocol has no leader term/epoch. Election and client messages need
an election generation so that delayed packets from an old primary cannot
overwrite newer state or convince clients to return to a stale leader.

### Performance

The implementation serializes each client's requests with stop-and-wait and
prints synchronously for every accepted or duplicate request. Creating an
unbounded new thread for every datagram is also expensive at the stated scale,
even though the supplied handout explicitly requests it. The expected
10-million-request workload needs a repeatable benchmark and likely batched or
disabled logging during performance tests, subject to the evaluator's interface
requirements.

### Input and protocol validation

- zero and negative request values are accepted despite the positive-integer
  requirement;
- text parsing has no explicit integer-width or overflow policy;
- malformed UTF-8 can terminate the server receive loop;
- ACK parsing assumes all fields exist and are valid;
- messages have no version, type envelope, sender replica ID, term, or
  authentication;
- the server's out-of-order diagnostic does not follow the timestamped interface
  format used for normal and duplicate requests.

### Operational packaging

The README examples use Windows path separators (`server\main.py`) even though
Linux execution is required. There is no automated launcher for multiple
replicas, no configuration format for replica addresses and IDs, and no
Makefile.

## Recommended implementation path

If the supplied 2025/1 Part 2 PDF is confirmed as authoritative:

1. Treat the Python repository as an executable protocol specification and
   rewrite the client/server in C or C++.
2. Define a typed wire protocol for discovery, client requests, ACKs,
   replication, heartbeats, election, election OK, coordinator announcement,
   and leader notification.
3. Preserve the full current logical state: aggregate counters plus every
   client's request/ACK state.
4. Add stable replica IDs and priorities, configured peer addresses, role
   (`PRIMARY`, `BACKUP`, `CANDIDATE`), election term, and state version.
5. Serialize state transitions under a mutex and replicate them in state-version
   order. Do not hold the state mutex across slow network waits unless the
   resulting blocking behavior is explicitly intended.
6. Make only the current primary answer client discovery and processing
   requests. Backups should redirect or ignore clients according to the chosen
   protocol.
7. Replace `RESET` during failover with leader rediscovery/notification and
   retransmission of the same request ID.
8. Implement Bully election timeouts and coordinator announcements, then test
   failure of the primary, highest-priority backup, and multiple replicas.
9. Add a Makefile, automated integration tests with packet loss/duplication, and
   a benchmark mode that can validate the final count and sum.

If the actual 2026/1 Part 2 specification permits Python, the existing code can
be extended rather than rewritten, but the server still needs a major
architectural change for ordered replication, election, client leader updates,
and thread-safe state.

## Overall suitability

- **As a Part 1 implementation:** good, compact, and mostly aligned with its
  source specification.
- **As a protocol/design prototype for Part 2:** good. The request IDs,
  duplicate handling, state structures, and discovery flow are valuable.
- **As code to submit under the supplied Part 2 PDF:** poor without a rewrite,
  because the language and server-concurrency requirements are unmet.
- **Estimated Part 2 functionality already present:** roughly 30% of the
  behavioral groundwork, but 0% of replication and leader election.

The immediate technical decision should be based on confirmation of the
authoritative Part 2 handout. That determines whether the next step is a C/C++
port or an in-place Python extension.
