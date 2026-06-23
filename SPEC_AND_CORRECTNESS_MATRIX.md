# Requirements and Correctness Matrix

## Scope and sources

This document evaluates the current repository against two different kinds of
authority:

1. The local [INF01151 Part 2 assignment](INF01151-Trabalho_202501_P2.pdf),
   which defines the required application and submission.
2. Hector Garcia-Molina, *Elections in a Distributed Computing System*, IEEE
   Transactions on Computers, C-31(1), 48-59, January 1982
   ([DOI](https://doi.org/10.1109/TC.1982.1675885)), which is the original
   source associated with the Bully election algorithm.

The original paper is the canonical source. The explicit message-by-message
formulation used below is also checked against Section 3 of the accessible
academic restatement
[Soundarabai et al., 2014](https://arxiv.org/abs/1403.3255), especially its
[Section 3 assumptions and algorithm steps](https://ar5iv.org/pdf/1403.3255).
That restatement is explanatory evidence, not a replacement for
Garcia-Molina's paper.

The assignment does not define election terms, dynamic server membership,
joining protocols, snapshot transfer, or exactly-once request identifiers.
Those are project extensions and require a separate correctness analysis. A
normal-path similarity to Bully is not by itself a proof that those extensions
are safe.

### Verdict vocabulary

**Conformance**

- **Conformant:** implements the source rule under its stated assumptions.
- **Extension:** behavior is outside the source, but does not inherently
  contradict it.
- **Partial:** implements the main path but omits a condition or guarantee.
- **Non-conformant:** contradicts an explicit rule or required property.
- **Not applicable:** the source does not define this behavior.

**Correctness**

- **Correct under stated assumptions:** the implementation and its tests
  support the property when the documented model holds.
- **Likely correct but unproven:** the design appears sound, but tests or a
  complete argument are missing.
- **Incorrect: concrete execution exists:** a valid event ordering violates
  the property.
- **Insufficient evidence:** the repository cannot establish the claim.
- **Outside the supported failure model:** the implementation explicitly
  excludes the scenario.

## Assignment requirements

| Requirement | PDF page | Coverage | Correctness | Acceptable deviation? | Evidence | Gap |
|---|---:|---|---|---|---|---|
| Distributed service sums integers from multiple clients into one accumulator | 1 | Covered | Correct under stated assumptions | Yes | Request commits update the global count and sum under a commit lock in [server/node.py](server/node.py#L637) and [server/state.py](server/state.py#L48). | End-to-end correctness still depends on election safety discussed below. |
| Each request contains a positive integer read from standard input | 1 | Covered | Correct under stated assumptions | Yes | Input accepts only `1..2^64-1` in [client/interface.py](client/interface.py#L18), and the server validates it again in [server/node.py](server/node.py#L620). | The PDF does not define an upper bound; unsigned 64-bit is a documented extension. |
| Display the resulting sum after every processed request | 1 | Covered | Correct under stated assumptions | Yes | The server prints request count and total after ACK-worthy processing in [server/node.py](server/node.py#L671); the client prints the acknowledged result in [client/interface.py](client/interface.py#L98). | `--quiet-requests` suppresses this output for benchmarks, which is reasonable outside evaluation mode. |
| Process requests concurrently, with one thread per received request | 1 | Covered | Correct under stated assumptions | Yes | Every `CLIENT_REQUEST` creates a worker thread in [server/node.py](server/node.py#L199). Shared commits are serialized by `commit_lock` in [server/state.py](server/state.py#L141). | Literal unbounded thread creation is expensive at the required scale. |
| Run on Unix/Linux | 1 | Covered | Correct under stated assumptions | Yes | The code uses Python's Unix-compatible socket and signal APIs; Linux instructions are documented in [README.md](README.md#L40). | A reproducible environment report is still missing. |
| Use Unix UDP sockets | 1 | Covered | Correct under stated assumptions | Yes | Clients and servers use `AF_INET/SOCK_DGRAM`; protocol datagrams are bounded in [common/protocol.py](common/protocol.py#L6). | The local benchmark relay is additional infrastructure, but request, replication, and election traffic remain UDP. |
| Implement the project in C or C++ | 1 | Not covered | Insufficient evidence | **No** | The repository is Python, explicitly acknowledged in [README.md](README.md#L12). | This is a direct specification violation unless the instructor granted an exception. |
| Preserve the correct sum as the primary evaluation criterion | 1 | Partially covered | Likely correct but unproven | Conditional | Unit/integration tests cover deduplication, replication, failover, and recovery. A persisted 10,000-operation chaos run ended with identical count, sum, version, and client hash on four replicas. | Election-safety counterexamples can create an isolated primary and lose acknowledged work. |
| Handle a large workload, with response time evaluated at 10,000,000+ requests | 1 | Not covered | Insufficient evidence | **No** | Benchmarks exist in [Makefile](Makefile#L23), and the README reports 100,000 operations, not 10,000,000, in [README.md](README.md#L232). | Run and report at least one valid 10,000,000-request evaluation. Per-request threads and all-backup ACKs are likely bottlenecks. |
| Use passive replication with one primary RM and one or more backup RMs | 1 | Covered | Likely correct but unproven | Yes | Roles, replicated state, and primary-only request handling are implemented in [server/state.py](server/state.py#L141) and [server/node.py](server/node.py#L630). | Safety depends on preventing two primaries, which is not currently guaranteed. |
| All clients use the same primary copy | 1 | Partially covered | Incorrect: concrete execution exists | **No** | Normally only `PRIMARY` answers discovery and backups redirect in [server/node.py](server/node.py#L574) and [server/node.py](server/node.py#L781). | A joining server can self-promote during an existing election and answer clients before learning the real coordinator. See Finding C2. |
| After each sum, the primary propagates state to backups | 1 | Covered | Correct under stated assumptions | Yes | A new operation is versioned, sent to every active backup, and ACKed before the client response in [server/node.py](server/node.py#L655) and [server/node.py](server/node.py#L687). | “State” is propagated as an ordered operation during normal service and as a full snapshot during synchronization. This is equivalent for replicas that apply every version. |
| Server failure must not make the service permanently unavailable | 1 | Covered | Likely correct but unproven | Yes | Primary heartbeats, timeout-triggered elections, snapshots, client retries, and tested failover exist in [server/node.py](server/node.py#L270), [server/node.py](server/node.py#L323), and [client/processing.py](client/processing.py#L42). | Liveness is not proven for all concurrent election/join schedules; the fresh chaos test did not converge. |
| Failover is transparent and participants are notified of the new server | 1-2 | Covered | Correct under stated assumptions | Yes | A coordinator broadcasts `COORDINATOR`, directly notifies known clients with `LEADER`, and clients rediscover after retries in [server/node.py](server/node.py#L960) and [client/processing.py](client/processing.py#L45). | UDP notifications are best effort, but rediscovery supplies the required fallback. |
| Secondary servers receive every modification needed to preserve consistency | 1 | Covered | Correct under stated assumptions | Yes | Replication includes request identity, value, address, and state version; backups reject gaps and request snapshots in [server/node.py](server/node.py#L721). Deduplication state is part of snapshots in [server/state.py](server/state.py#L103). | Correct only while a unique primary exists. |
| On primary failure, use the Bully algorithm to elect the next manager | 2 | Partially covered | Incorrect: concrete execution exists | **No** | The normal election path sends to higher IDs, handles `OK`, and announces a coordinator in [server/node.py](server/node.py#L829). | Term interaction permits a stale candidate to promote itself after accepting another coordinator. See Finding C1. |
| A secondary promoted after failure maintains a consistent system state | 2 | Covered | Likely correct but unproven | Conditional | The candidate collects state versions, selects the greatest, installs its snapshot, and snapshots survivors before announcing in [server/node.py](server/node.py#L886). | Snapshot completion is not tied to the expected transfer, and failed survivor synchronization is ignored. See Findings C3 and C4. |
| Update clients with the newly elected leader | 2 | Covered | Correct under stated assumptions | Yes | `announce_coordinator()` broadcasts the cluster result and sends `LEADER` to addresses retained in replicated client state in [server/node.py](server/node.py#L960). | Clients not yet known to the server rely on repeated discovery, which is sufficient. |
| Report the OS/distribution, hardware, memory, and compiler versions | 2 | Not covered | Not applicable | **No** | The tracked [Relatorio.pdf](Relatorio.pdf) is the Part 1 report and does not contain the required Part 2 environment. | Produce a Part 2 report with the requested environment. |
| Report and justify the election algorithm | 2 | Not covered | Not applicable | **No** | The README gives an operational summary, but the submitted report describes the old single-server implementation. | Explain the implemented Bully variant, assumptions, extensions, and known deviations. |
| Report passive replication and its implementation challenges | 2 | Not covered | Not applicable | **No** | The current report predates replication. | Document commit ordering, ACK policy, deduplication, snapshots, and recovery. |
| Report implementation problems and how they were or were not solved | 2 | Not covered | Not applicable | **No** | Only Part 1 problems are reported. | Include Part 2 failures, testing evidence, and unresolved election races. |
| Match the requested interface exactly | 2 | Partially covered | Insufficient evidence | Conditional | The current client/server output is structured and documented, but the Part 2 handout refers to an earlier interface specification that is not the matching 2025/1 Part 1 handout in this repository. | Confirm the authoritative Part 1 interface with the instructor and compare it literally. |
| Provide automated compilation/build scripts such as a Makefile | 2 | Covered | Correct under stated assumptions | Yes | [Makefile](Makefile#L1) provides run, test, generation, and benchmark targets. | For Python this launches/checks rather than compiles; this does not cure the C/C++ violation. |
| Include source code, compilation/execution tutorial, and report in a ZIP | 2 | Partially covered | Not applicable | Conditional | Source, Makefile, README tutorial, and a report are tracked. | The report is outdated and ZIP packaging cannot be established from the repository. |
| Team of four, clearly identified in report and presentation | 2 | Not covered | Not applicable | N/A | The existing report identifies two people. | Administrative requirement; update team information as applicable. |
| Demonstrate the working system in person; each member must attend and understand their work | 2 | Not assessable | Not applicable | N/A | Cannot be inferred from code. | Presentation and attendance are external obligations. |

## Classical Bully conformance

The classical algorithm assumes a synchronous system, unique ordered process
IDs, knowledge of every process and its address, crash/recovery failures,
timeouts, and reliable time-bounded delivery. Its goal is agreement on the
highest-priority live process. The assignment itself does not restate all of
these assumptions, but asking for the Bully algorithm imports them unless a
different model is explicitly justified.

| Paper rule/property | Paper assumption | Current mechanism | Conformance | Correctness verdict | Counterexample or justification |
|---|---|---|---|---|---|
| Every process has a unique, non-null, totally ordered identifier | IDs are unique and comparable | Positive unsigned 64-bit `server_id`; random by default or supplied by CLI in [server/main.py](server/main.py#L25). | Conformant | Likely correct but unproven | Values are validated, but duplicate operator-supplied IDs are not detected cluster-wide. Correctness therefore assumes IDs are unique. |
| Every process knows every other eligible process and address | Fixed, known process set | Each server keeps a dynamic `members` map received through snapshots, heartbeats, and membership broadcasts in [server/state.py](server/state.py#L159). | Partial | Likely correct but unproven | The implementation replaces fixed knowledge with eventually synchronized membership. The original proof does not cover incomplete or changing membership. |
| System is synchronous and failures are detected by timeout | Reliable, time-bounded communication | Primary heartbeat every 500 ms and a 2-second timeout trigger elections in [server/node.py](server/node.py#L270) and [server/node.py](server/node.py#L323). | Conformant | Correct under stated assumptions | Works if timeout bounds dominate scheduling and network delay. Partitions are explicitly out of scope. |
| Message delivery is reliable and time bounded | No permanent loss between live processes | UDP plus repeated client, replication, snapshot, and discovery messages approximates reliability. | Partial | Likely correct but unproven | Election messages themselves are sent once per election attempt. Timeouts retry the election, but the implementation is not literally a reliable channel. |
| A process detecting coordinator failure initiates an election | A failure detector eventually suspects a crashed coordinator | Every non-primary with a known leader calls `start_election()` after heartbeat timeout in [server/node.py](server/node.py#L337). | Conformant | Correct under stated assumptions | Concurrent election initiators are expected by Bully. |
| Send `ELECTION` to every higher-numbered live-or-unknown process | Complete process knowledge | `start_election()` targets higher-ID entries currently marked `ACTIVE` in [server/node.py](server/node.py#L841). | Partial | Likely correct but unproven | It omits unknown, `JOINING`, and absent-from-membership higher processes. That is valid only if `ACTIVE` membership is complete and synchronized. |
| If no higher process answers in time, become coordinator | Bounded reliable reply delivery | Candidate waits for `ELECTION_OK`, then calls `become_coordinator()` in [server/node.py](server/node.py#L854). | Partial | Incorrect: concrete execution exists | The transition lacks a final check that the node is still candidate in the same term. A node can self-elect after accepting a newer coordinator. |
| A higher process receiving `ELECTION` sends `OK` | Receiver is live and has greater priority | `handle_election()` sends `ELECTION_OK` when its ID is greater in [server/node.py](server/node.py#L865). | Partial | Incorrect: concrete execution exists | The reply carries the receiver's current term, not necessarily the request term. The sender indexes its wait event by its own term, so the valid higher response can be ignored. |
| The higher receiver starts its own election unless already conducting one | Elections may overlap | The receiver launches `start_election()`; `election_lock` suppresses a second local election in [server/node.py](server/node.py#L829) and [server/node.py](server/node.py#L876). | Conformant | Correct under stated assumptions | Concurrent elections across different processes are normal Bully behavior. |
| After receiving `OK`, stop competing and wait for a coordinator | A higher process takes responsibility | The candidate waits for `coordinator_announced`, then schedules a new election on timeout in [server/node.py](server/node.py#L854). | Conformant | Likely correct but unproven | It works when `OK` and coordinator messages use the expected term. Term mismatch breaks the handoff. |
| If no coordinator arrives after an `OK`, retry the election | Coordinator may crash during election | A timer calls `start_election()` after `COORDINATOR_TIMEOUT` in [server/node.py](server/node.py#L855). | Conformant | Correct under stated assumptions | The retry is present. |
| The winner announces itself to every process | Complete process knowledge or reliable broadcast | `announce_coordinator()` broadcasts `COORDINATOR` with leader and membership in [server/node.py](server/node.py#L960). | Conformant | Correct under stated assumptions | Broadcast/relay delivery must eventually reach all live servers. |
| All live processes accept the announced coordinator | Coordinator is the valid highest live process | `handle_coordinator()` accepts any non-stale term and becomes backup in [server/node.py](server/node.py#L982). | Partial | Incorrect: concrete execution exists | Equal-term announcements have no deterministic winner check. Once two coordinators exist in one term, each can demote the other or produce inconsistent views. |
| A recovered process with a higher ID than the coordinator runs Bully | Recovery is detectable and process knowledge is fixed | A joining server first installs a snapshot, becomes active, and then starts election if its ID is higher in [server/node.py](server/node.py#L492) and [server/node.py](server/node.py#L524). | Extension | Correct under stated assumptions | Synchronizing before eligibility is stronger than immediately bullying because it prevents a stale recovered process from serving state. |
| The highest live eligible process is eventually coordinator | Stable synchronous period and complete membership | Higher IDs answer lower candidates and a synchronized higher join triggers election. | Partial | Likely correct but unproven | The property holds on the tested normal path, but incomplete membership can leave a higher active process uninvolved, and a lower `COORDINATOR` is accepted without triggering a challenge. |
| Simultaneous elections converge | Reliable bounded communication and fixed process set | Each candidate contacts higher active IDs; higher candidates recursively run elections. | Partial | Incorrect: concrete execution exists | Classical overlap is expected, but independently incremented terms can cause `OK` to be ignored and allow stale promotion. Terms are an extension whose rules are incomplete. |

## Extended-system correctness

### Invariant matrix

| Invariant | Current mechanism | Verdict | Reasoning |
|---|---|---|---|
| At most one active primary per election term | Terms reject messages with lower terms; coordinator messages set other nodes to backup. | **Incorrect: concrete execution exists** | `become_coordinator(term)` does not verify that the node is still `CANDIDATE` or that its current term still equals `term`. Finding C1 creates two primaries in term 3. |
| Eventual leader election after leader failure | Heartbeat timeout starts Bully; `OK` and coordinator waits have retry timers. | **Likely correct but unproven** | Normal failover tests pass, but term mismatch and stale promotion can prolong or destabilize elections. |
| Highest synchronized active server eventually wins | Elections target higher `ACTIVE` members; a higher server challenges after snapshot installation. | **Likely correct but unproven** | Correct with complete converged membership. The original Bully proof cannot be applied while membership is incomplete or changing. |
| A joining server cannot become leader before synchronization | Existing primaries snapshot joiners before marking them active. | **Incorrect: concrete execution exists** | `check_startup()` independently turns a `JOINING` server into `PRIMARY` after 1.2 seconds if it has not learned a leader and appears highest among only the IDs it happens to know. Finding C2 shows the violation. |
| Only the current leader can replicate state | Backups require the sender to equal `leader_id`; clients are accepted only in `PRIMARY`. | **Correct under stated assumptions** | Checks exist in [server/node.py](server/node.py#L630) and [server/node.py](server/node.py#L721). This property collapses if election safety allows conflicting leader views. |
| Every acknowledged operation survives failover | Primary applies once, replicates to all active backups, waits for every ACK, then replies. | **Correct under stated assumptions** | With one primary and at least one synchronized survivor, every client ACK corresponds to replicated deduplication and sum state. |
| Each `(client_id, request_id)` is summed at most once | Per-client last request and saved ACK are replicated; duplicates return the prior ACK. | **Correct under stated assumptions** | Classification and commit serialization in [server/state.py](server/state.py#L40) prevent duplicate application within one state lineage. Conflicting primaries can create different lineages. |
| No logically submitted operation is lost | Client retains the same pending ID/value until ACK and rediscovery. Election recovery selects the highest surviving state version. | **Correct under stated assumptions** | An unacknowledged request may be applied again on a new lineage, but deduplication ensures the logical operation contributes once to the surviving state. An ACK from an isolated startup primary violates the premise. |
| Replicas converge to one state prefix | Versions enforce contiguous operation replication; snapshots replace older state; coordinator selects highest version. | **Likely correct but unproven** | This works under one primary. Snapshot completion and failed synchronization handling leave recovery races, Findings C3 and C4. |
| Clients preserve a pending request through elections | Stop-and-wait rebuilds neither ID nor payload while rediscovering. | **Correct under stated assumptions** | [client/processing.py](client/processing.py#L23) encodes the payload once outside the retry loop and only returns on the matching ACK. |
| Membership changes cannot invalidate an election in progress | Election takes one snapshot of `active_members`; no election membership version is frozen or checked. | **Incorrect: concrete execution exists** | A higher active server can be added or omitted after the candidate selects targets. The candidate can then announce without proving it considered the membership version on which eligibility was based. |

### Concrete correctness findings

#### C1. Critical: stale candidate can become coordinator after accepting a newer coordinator

1. Server 20 starts election term 2 and sends `ELECTION` to server 30.
2. Server 30 is already conducting term 3 and broadcasts `COORDINATOR(30, term=3)`.
3. Server 20 receives it, records term 3, leader 30, and role `BACKUP`.
4. Server 30 later receives server 20's term-2 `ELECTION`. It answers using its
   current term 3 because `server_message()` always reads current state.
5. Server 20 is waiting on `election_ok[2]`, so `ELECTION_OK(term=3)` does not
   set its event.
6. Its original election thread times out and calls `become_coordinator(2)`.
   That method does not recheck role or term. It sets leader 20, preserves
   `max(3, 2) = 3`, becomes `PRIMARY`, and announces itself in term 3.

The result is two possible primaries in the same term. This is valid even on a
reliable network because it needs only message reordering and concurrent
elections, both explicitly possible in classical Bully.

Relevant code:
[start_election](server/node.py#L829),
[term-changing election handler](server/node.py#L865),
[term-keyed OK handler](server/node.py#L880),
[unguarded coordinator transition](server/node.py#L886), and
[coordinator acceptance](server/node.py#L982).

#### C2. Critical: a joining server can self-promote beside an existing cluster

1. The established leader fails and existing active servers are `CANDIDATE`.
2. A new server starts as `JOINING` and broadcasts `SERVER_HELLO`.
3. Candidates do not answer join hellos and emit neither primary nor backup
   heartbeat.
4. After `STARTUP_SETTLE`, the joiner has no `leader_id`. If its incomplete
   local member set contains no higher ID, `check_startup()` promotes it
   directly to `PRIMARY` without a snapshot.
5. It answers discovery and can ACK client operations with no active backups.
6. The established cluster later elects its own leader. The joiner's
   acknowledged operations are absent from that cluster and can be lost when
   it accepts the higher-term coordinator.

Reliable delivery does not prevent this schedule because there is no response
for the join hello while all established servers are candidates. The problem
is premature eligibility, not packet loss.

Relevant code:
[role-specific announcements](server/node.py#L256),
[startup promotion](server/node.py#L299), and
[join handling restricted to a primary](server/node.py#L363).

#### C3. High: snapshot completion is not associated with the awaited transfer

`become_coordinator()` clears and waits on one global `snapshot_installed`
event after requesting the freshest peer's snapshot. Every successfully
installed snapshot sets that event, regardless of transfer ID, expected source,
or election term.

A joining/recovery snapshot from another sender can therefore release the wait
before the requested freshest snapshot is installed. The candidate may then
become primary with an older state. The receiver validates each transfer's
chunks, but the completion signal does not identify which transfer completed.

Relevant code:
[global event creation](server/node.py#L102),
[all snapshots setting it](server/node.py#L461), and
[coordinator wait](server/node.py#L902).

This needs overlapping valid snapshot transfers, so it is less likely than C1
or C2, but it is a concrete process-concurrency execution and does not require
network corruption.

#### C4. High: coordinator announces even when survivor synchronization fails

After selecting state, the candidate calls `send_snapshot()` for each active
peer but ignores the returned success value. It then becomes primary and
announces the coordinator. A live peer that misses all finite snapshot retries
can remain active with stale state until another repair path happens.

This is not a violation under the original paper's reliable time-bounded
channel, but it conflicts with the project's stronger claim to tolerate UDP
loss and eventual recovery. The fresh chaos-test timeout is consistent with a
convergence problem, although it does not by itself prove this exact path.

Relevant code:
[finite snapshot retries](server/node.py#L440) and
[ignored synchronization result](server/node.py#L934).

#### C5. Medium: equal-term leader messages have no deterministic tie-break

Heartbeat and coordinator handlers reject only lower terms. They accept a
different leader in the same term and demote the local process without checking
server priority or a unique election identity. This makes the split created by
C1 unstable: two same-term primaries can successively accept each other's
messages, disagree temporarily, or leave no stable primary.

Relevant code:
[heartbeat acceptance](server/node.py#L792) and
[coordinator acceptance](server/node.py#L982).

#### C6. Medium: elections are not bound to a membership version

An election selects higher peers and recovery peers from the mutable local
membership table. Messages contain an election term but no membership version
that defines the eligible set. A server joining, becoming active, being
removed, or being absent from one candidate's table during that interval can
change who should win without invalidating the in-progress election.

For example:

1. The primary activates higher-ID server 30 and broadcasts the new membership.
2. Backup 20 has not processed that membership datagram when the primary fails.
3. Server 20 times out first, snapshots its old member table, finds no higher
   active server, and begins the no-`OK` path.
4. The delayed membership update or another cluster message makes server 30
   known, but server 20's election does not revalidate its target set or
   membership version before `become_coordinator()`.
5. Server 20 can announce despite not having challenged known higher server 30.

This is outside classical Bully's fixed-membership proof. Under the project's
dynamic model, correctness requires either freezing an election configuration
or rediscovering and confirming the survivor set before coordinator
announcement.

Relevant code:
[dynamic active-member query](server/state.py#L159),
[election target selection](server/node.py#L841), and
[recovery peer selection](server/node.py#L886).

## Test evidence

Evidence must not be confused with a proof:

- On June 15, 2026, `make test` passed 13 of 14 tests. Protocol, state,
  heartbeat, idle backup removal/rejoin, replication interruption, failover,
  and higher-priority join tests passed.
- The fresh subprocess chaos test failed after about 67 seconds with
  `TimeoutError('timed out waiting for final replicated state')`. During that
  run the observed server version exceeded the generated 10,000-operation
  workload, so the failure requires investigation before using that run as
  correctness evidence.
- Earlier persisted report
  [chaos-20260614-221013-b81e9908](benchmark-results/chaos-20260614-221013-b81e9908/report.json)
  passed 10,000 operations in about 30 seconds. Four final replicas had
  `num_reqs=10000`, `total_sum=511940`, `state_version=10000`, one identical
  client-state hash, and leader 60.
- Existing integration coverage in
  [testing/test_cluster.py](testing/test_cluster.py#L69) validates important
  normal and failure paths, but it does not force the C1-C6 event orderings.

## Prioritized conclusions

1. **Election safety: incorrect.** C1 provides a reliable-network execution
   with two primaries in one term. C2 provides a second route to temporary
   split leadership and possible acknowledged-state loss.
2. **Failover correctness: conditional.** It is strong once a unique leader
   and synchronized membership are assumed, but those premises are not always
   preserved by the election/join protocol.
3. **Replicated-sum safety: conditionally correct.** Commit serialization,
   all-active-backup ACKs, versions, and replicated deduplication are sound
   within one leader lineage. Election safety can create multiple lineages.
4. **Election liveness: likely but unproven.** Timeouts and retries exist and
   normal failover passes, but same-term conflicts and changing membership can
   delay convergence. The fresh chaos failure prevents a stronger verdict.
5. **Classical Bully conformance: partial.** The normal message flow is
   recognizable and mostly faithful. Dynamic membership and terms are
   extensions, and their incomplete interaction invalidates the classical
   safety argument.
6. **Assignment compliance: substantial but not complete.** Core application,
   UDP, threading, passive replication, notification, Makefile, and operational
   documentation exist. Python instead of C/C++, the obsolete Part 1 report,
   absence of a 10,000,000-request result, and the election-safety defects are
   material non-compliances.

The implementation should therefore not currently be described as a correct
Bully implementation without qualification. A precise description is:
“a Bully-inspired, term-extended dynamic-membership protocol whose normal path
works in tests, but whose election safety is not preserved for all reliable
concurrent executions.”
