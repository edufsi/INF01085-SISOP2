from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import random
import signal
import socket
import subprocess
import sys
import threading
import time
import uuid
from typing import Any, Callable


ROOT = Path(__file__).resolve().parents[1]
PYTHON = sys.executable
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from common.protocol import ProtocolError, decode, encode, message


INITIAL_SERVER_IDS = (10, 20, 30, 40)
ALL_SERVER_IDS = (10, 20, 30, 40, 50, 60)


@dataclass
class ChaosConfig:
    output_root: Path = Path("benchmark-results")
    seed: int = 20260613
    entries_per_client: int = 25_000
    target_duration: float = 240.0
    base_port: int = 47000
    calibration_requests_per_client: int = 200
    stage_timeout: float = 35.0
    completion_timeout: float = 1_200.0
    sample_interval: float = 1.0
    zero_client_pause: float = 2.0
    warning_seconds: float = 270.0
    quick: bool = False


class RunRecorder:
    def __init__(self, run_dir: Path) -> None:
        self.run_dir = run_dir
        self.run_dir.mkdir(parents=True, exist_ok=True)
        self.started = time.monotonic()
        self.lock = threading.Lock()
        self.timeline: list[dict[str, Any]] = []
        self.timeline_path = run_dir / "timeline.jsonl"

    def record(self, event: str, **fields: Any) -> dict[str, Any]:
        item = {
            "elapsed": round(time.monotonic() - self.started, 6),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "event": event,
            **fields,
        }
        with self.lock:
            self.timeline.append(item)
            with self.timeline_path.open("a", encoding="utf-8") as output:
                output.write(json.dumps(item, sort_keys=True) + "\n")
        print(
            f"[{item['elapsed']:8.3f}s] {event}"
            + (f" {json.dumps(fields, sort_keys=True)}" if fields else ""),
            flush=True,
        )
        return item


@dataclass
class ProcessRecord:
    key: str
    component: str
    identity: str
    generation: int
    pid: int
    command: list[str]
    log_path: str
    started_at: str
    stopped_at: str | None = None
    signal: str | None = None
    exit_code: int | None = None


class ProcessManager:
    def __init__(self, logs_dir: Path, recorder: RunRecorder) -> None:
        self.logs_dir = logs_dir
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        self.recorder = recorder
        self.processes: dict[str, subprocess.Popen[bytes]] = {}
        self.records: list[ProcessRecord] = []
        self.generations: dict[tuple[str, str], int] = {}
        self.log_handles: dict[str, Any] = {}

    def start(
        self,
        component: str,
        identity: str,
        command: list[str],
    ) -> ProcessRecord:
        generation_key = (component, identity)
        generation = self.generations.get(generation_key, 0) + 1
        self.generations[generation_key] = generation
        key = f"{component}:{identity}"
        current = self.processes.get(key)
        if current is not None and current.poll() is None:
            raise RuntimeError(f"{key} is already running")
        log_path = self.logs_dir / f"{component}_{identity}_generation_{generation}.log"
        log_handle = log_path.open("ab", buffering=0)
        process = subprocess.Popen(
            command,
            cwd=ROOT,
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        self.processes[key] = process
        self.log_handles[key] = log_handle
        record = ProcessRecord(
            key=key,
            component=component,
            identity=identity,
            generation=generation,
            pid=process.pid,
            command=command,
            log_path=str(log_path),
            started_at=datetime.now(timezone.utc).isoformat(),
        )
        self.records.append(record)
        self.recorder.record(
            "process_started",
            component=component,
            identity=identity,
            generation=generation,
            pid=process.pid,
            command=command,
        )
        return record

    def running(self, component: str, identity: str) -> bool:
        process = self.processes.get(f"{component}:{identity}")
        return process is not None and process.poll() is None

    def running_identities(self, component: str) -> set[str]:
        return {
            identity
            for key, process in self.processes.items()
            for prefix, identity in [key.split(":", 1)]
            if prefix == component and process.poll() is None
        }

    def stop(
        self,
        component: str,
        identity: str,
        *,
        graceful: bool,
        timeout: float = 10.0,
    ) -> None:
        key = f"{component}:{identity}"
        process = self.processes[key]
        if process.poll() is None:
            sent_signal = signal.SIGTERM if graceful else signal.SIGKILL
            os.killpg(process.pid, sent_signal)
            try:
                process.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait(timeout=5.0)
                sent_signal = signal.SIGKILL
        else:
            sent_signal = None
        record = next(
            item
            for item in reversed(self.records)
            if item.key == key and item.stopped_at is None
        )
        record.stopped_at = datetime.now(timezone.utc).isoformat()
        record.signal = (
            signal.Signals(sent_signal).name if sent_signal is not None else None
        )
        record.exit_code = process.returncode
        handle = self.log_handles.pop(key, None)
        if handle is not None:
            handle.close()
        self.recorder.record(
            "process_stopped",
            component=component,
            identity=identity,
            pid=process.pid,
            mode="graceful" if graceful else "abrupt",
            exit_code=process.returncode,
        )

    def stop_all(self) -> None:
        for key, process in list(self.processes.items()):
            if process.poll() is None:
                component, identity = key.split(":", 1)
                try:
                    self.stop(component, identity, graceful=False, timeout=2.0)
                except (ProcessLookupError, subprocess.TimeoutExpired):
                    pass
        for handle in self.log_handles.values():
            handle.close()
        self.log_handles.clear()

    def records_payload(self) -> list[dict[str, Any]]:
        for record in self.records:
            process = self.processes.get(record.key)
            if (
                process is not None
                and record.stopped_at is None
                and process.poll() is not None
            ):
                record.stopped_at = datetime.now(timezone.utc).isoformat()
                record.exit_code = process.returncode
        return [asdict(record) for record in self.records]


class UDPStatusClient:
    def __init__(self) -> None:
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind(("127.0.0.1", 0))

    def query(
        self, address: tuple[str, int], timeout: float = 0.5
    ) -> dict[str, Any] | None:
        request_id = uuid.uuid4().hex
        self.socket.sendto(
            encode(message("STATUS_REQUEST", request_id=request_id)),
            address,
        )
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            self.socket.settimeout(max(0.001, deadline - time.monotonic()))
            try:
                data, _ = self.socket.recvfrom(2048)
            except socket.timeout:
                return None
            try:
                payload = decode(data)
            except ProtocolError:
                continue
            if (
                payload["type"] == "STATUS_RESPONSE"
                and payload.get("request_id") == request_id
            ):
                return payload
        return None

    def close(self) -> None:
        self.socket.close()


def wait_until(predicate: Callable[[], bool], timeout: float, description: str) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.05)
    raise TimeoutError(f"timed out waiting for {description}")


class ProcessChaosBenchmark:
    def __init__(self, config: ChaosConfig) -> None:
        self.config = config
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        self.run_dir = config.output_root / f"chaos-{timestamp}-{uuid.uuid4().hex[:8]}"
        self.recorder = RunRecorder(self.run_dir)
        self.manager = ProcessManager(self.run_dir / "logs", self.recorder)
        self.status_client = UDPStatusClient()
        self.server_port = config.base_port
        self.relay_address = ("127.0.0.1", config.base_port + 100)
        self.server_addresses = {
            server_id: (f"127.0.0.{server_id}", self.server_port)
            for server_id in ALL_SERVER_IDS
        }
        self.client_addresses = {
            index: (f"127.0.1.{index + 1}", self.server_port)
            for index in range(4)
        }
        self.assertions: list[dict[str, Any]] = []
        self.observed_server_counts: set[int] = set()
        self.observed_client_counts: set[int] = set()
        self.samples: list[dict[str, Any]] = []
        self.monitor_stop = threading.Event()
        self.monitor_thread: threading.Thread | None = None
        self.profile_throughput = 0.0
        self.projected_duration = 0.0
        self.manifest: dict[str, Any] = {}
        self.last_client_states: dict[int, dict[str, Any]] = {}
        self.measured_started: float | None = None
        self.warning_recorded = False

    def assertion(self, name: str, passed: bool, detail: Any) -> None:
        item = {"name": name, "passed": passed, "detail": detail}
        self.assertions.append(item)
        self.recorder.record("assertion", **item)
        if not passed:
            raise AssertionError(f"{name}: {detail}")

    @staticmethod
    def endpoint_text(address: tuple[str, int]) -> str:
        return f"{address[0]}:{address[1]}"

    def start_relay(self, label: str, address: tuple[str, int]) -> None:
        self.manager.start(
            "relay",
            label,
            [
                PYTHON,
                "-u",
                "benchmarks/relay.py",
                "--bind",
                self.endpoint_text(address),
                "--expiry",
                "1.5" if self.config.quick else "5.0",
            ],
        )
        time.sleep(0.1)

    def start_server(
        self,
        server_id: int,
        *,
        port: int | None = None,
        relay: tuple[str, int] | None = None,
    ) -> None:
        service_port = port or self.server_port
        relay_address = relay or self.relay_address
        host = f"127.0.0.{server_id}"
        self.manager.start(
            "server",
            str(server_id),
            [
                PYTHON,
                "-u",
                "server/main.py",
                str(service_port),
                "--server-id",
                str(server_id),
                "--bind",
                host,
                "--discovery",
                self.endpoint_text(relay_address),
                "--quiet-requests",
            ],
        )

    def start_client(
        self,
        index: int,
        *,
        relay: tuple[str, int] | None = None,
        port: int | None = None,
        calibration: bool = False,
        resume: dict[str, Any] | None = None,
        new_identity: bool = False,
        startup_hold: float = 0.0,
    ) -> None:
        address = (
            self.client_addresses[index][0],
            port or self.client_addresses[index][1],
        )
        command = [
            PYTHON,
            "-u",
            "benchmarks/client_worker.py",
            "--seed",
            str(self.config.seed),
            "--client-index",
            str(index),
            "--count",
            str(
                self.config.calibration_requests_per_client
                if calibration
                else self.config.entries_per_client
            ),
            "--bind",
            self.endpoint_text(address),
            "--discovery",
            self.endpoint_text(relay or self.relay_address),
        ]
        if resume is not None:
            command.extend(["--position", str(resume["position"])])
            if not new_identity:
                command.extend(
                    [
                        "--client-id",
                        str(resume["client_id"]),
                        "--request-id",
                        str(resume["next_request_id"]),
                    ]
                )
                pending = resume.get("pending")
                if pending is not None:
                    command.extend(["--pending-value", str(pending["value"])])
        if startup_hold > 0:
            command.extend(["--startup-hold", str(startup_hold)])
        self.manager.start("client", str(index), command)

    def server_status(self, server_id: int) -> dict[str, Any] | None:
        return self.status_client.query(self.server_addresses[server_id], 0.35)

    def client_status(self, index: int) -> dict[str, Any] | None:
        return self.status_client.query(self.client_addresses[index], 0.35)

    def statuses(
        self, component: str, identities: set[int]
    ) -> dict[int, dict[str, Any]]:
        getter = self.server_status if component == "server" else self.client_status
        result: dict[int, dict[str, Any]] = {}
        for identity in identities:
            status = getter(identity)
            if status is not None:
                result[identity] = status
        return result

    def active_server_ids(self) -> set[int]:
        return {
            int(identity)
            for identity in self.manager.running_identities("server")
        }

    def active_client_ids(self) -> set[int]:
        return {
            int(identity)
            for identity in self.manager.running_identities("client")
        }

    def wait_topology(
        self, expected_ids: set[int], expected_leader: int, timeout: float
    ) -> None:
        latest: dict[int, dict[str, Any]] = {}

        def converged() -> bool:
            nonlocal latest
            if self.active_server_ids() != expected_ids:
                return False
            latest = self.statuses("server", expected_ids)
            if set(latest) != expected_ids:
                return False
            leaders = [
                server_id
                for server_id, status in latest.items()
                if status["role"] == "PRIMARY"
            ]
            return (
                leaders == [expected_leader]
                and all(
                    set(status["active_server_ids"]) == expected_ids
                    and status["leader_id"] == expected_leader
                    for status in latest.values()
                )
            )

        wait_until(converged, timeout, f"topology {expected_ids}/{expected_leader}")
        self.observed_server_counts.add(len(expected_ids))
        self.recorder.record(
            "topology_converged",
            active_servers=sorted(expected_ids),
            leader_id=expected_leader,
            state_versions={
                server_id: status["state_version"]
                for server_id, status in latest.items()
            },
        )

    def total_progress(
        self, known_statuses: dict[int, dict[str, Any]] | None = None
    ) -> int:
        total = 0
        for index in range(4):
            status = (
                known_statuses.get(index)
                if known_statuses is not None
                else self.client_status(index)
            )
            if status is not None:
                total += int(status["position"])
                continue
            previous = self.last_client_states.get(index)
            if previous is not None:
                total += int(previous["position"])
        return total

    def wait_progress(self, stage: int) -> None:
        lifecycle_target = min(40_000, max(400, int(self.manifest["total_count"] * 0.4)))
        target = max(4, int(lifecycle_target * stage / 10))
        wait_until(
            lambda: self.total_progress() >= target,
            self.config.stage_timeout + self.config.target_duration / 10,
            f"progress {target}",
        )
        self.recorder.record(
            "progress_milestone",
            stage=stage,
            target=target,
            acknowledged=self.total_progress(),
        )

    def stop_client(self, index: int, *, graceful: bool) -> dict[str, Any] | None:
        before = self.client_status(index)
        if graceful:
            process = self.manager.processes[f"client:{index}"]
            os.killpg(process.pid, signal.SIGTERM)
            deadline = time.monotonic() + 3.0
            while time.monotonic() < deadline and process.poll() is None:
                latest = self.client_status(index)
                if latest is not None:
                    before = latest
                time.sleep(0.02)
        self.manager.stop("client", str(index), graceful=graceful)
        if before is not None:
            self.last_client_states[index] = before
        self.observed_client_counts.add(len(self.active_client_ids()))
        return before

    def stop_server(self, server_id: int, *, graceful: bool) -> None:
        self.manager.stop("server", str(server_id), graceful=graceful)
        self.observed_server_counts.add(len(self.active_server_ids()))

    def start_monitor(self) -> None:
        path = self.run_dir / "samples.jsonl"
        status_path = self.run_dir / "status_snapshots.jsonl"

        def monitor() -> None:
            last_progress = 0
            last_time = time.monotonic()
            while not self.monitor_stop.wait(self.config.sample_interval):
                now = time.monotonic()
                servers = sorted(self.active_server_ids())
                clients = sorted(self.active_client_ids())
                self.observed_server_counts.add(len(servers))
                self.observed_client_counts.add(len(clients))
                server_statuses = self.statuses("server", set(servers))
                client_statuses = self.statuses("client", set(clients))
                progress = self.total_progress(client_statuses)
                leaders = [
                    server_id
                    for server_id, status in server_statuses.items()
                    if status["role"] == "PRIMARY"
                ]
                sample = {
                    "elapsed": round(now - self.recorder.started, 6),
                    "acknowledged": progress,
                    "interval_throughput": (progress - last_progress)
                    / max(now - last_time, 0.001),
                    "active_servers": servers,
                    "active_clients": clients,
                    "leader_id": leaders[0] if len(leaders) == 1 else None,
                }
                self.samples.append(sample)
                with path.open("a", encoding="utf-8") as output:
                    output.write(json.dumps(sample, sort_keys=True) + "\n")
                with status_path.open("a", encoding="utf-8") as output:
                    output.write(
                        json.dumps(
                            {
                                "elapsed": sample["elapsed"],
                                "servers": server_statuses,
                                "clients": client_statuses,
                            },
                            sort_keys=True,
                        )
                        + "\n"
                    )
                if (
                    self.measured_started is not None
                    and not self.warning_recorded
                    and now - self.measured_started >= self.config.warning_seconds
                    and progress < int(self.manifest.get("total_count", 0))
                ):
                    self.warning_recorded = True
                    self.recorder.record(
                        "duration_warning",
                        measured_elapsed=now - self.measured_started,
                        acknowledged=progress,
                    )
                last_progress = progress
                last_time = now

        self.monitor_thread = threading.Thread(target=monitor, daemon=True)
        self.monitor_thread.start()

    def profile(self) -> float:
        calibration_port = self.config.base_port + 200
        relay = ("127.0.0.1", calibration_port + 100)
        original_server_addresses = self.server_addresses
        original_client_addresses = self.client_addresses
        self.server_addresses = {
            **self.server_addresses,
            **{
                server_id: (f"127.0.0.{server_id}", calibration_port)
                for server_id in INITIAL_SERVER_IDS
            },
        }
        self.client_addresses = {
            index: (f"127.0.1.{index + 1}", calibration_port)
            for index in range(4)
        }
        self.start_relay("calibration", relay)
        try:
            for server_id in INITIAL_SERVER_IDS:
                self.start_server(server_id, port=calibration_port, relay=relay)
            self.wait_topology(
                set(INITIAL_SERVER_IDS),
                40,
                self.config.stage_timeout,
            )
            started = time.monotonic()
            for index in range(4):
                self.start_client(
                    index,
                    relay=relay,
                    port=calibration_port,
                    calibration=True,
                )
            wait_until(
                lambda: all(
                    (status := self.client_status(index)) is not None
                    and status["completed"]
                    for index in range(4)
                ),
                self.config.stage_timeout * 2,
                "calibration completion",
            )
            elapsed = max(time.monotonic() - started, 0.001)
            completed = self.config.calibration_requests_per_client * 4
            throughput = completed / elapsed
            self.recorder.record(
                "calibration_complete",
                requests=completed,
                calibration_elapsed=elapsed,
                throughput=throughput,
            )
            return throughput
        finally:
            for index in range(4):
                if self.manager.running("client", str(index)):
                    self.manager.stop("client", str(index), graceful=False)
            for server_id in INITIAL_SERVER_IDS:
                if self.manager.running("server", str(server_id)):
                    self.manager.stop("server", str(server_id), graceful=False)
            if self.manager.running("relay", "calibration"):
                self.manager.stop("relay", "calibration", graceful=False)
            self.server_addresses = original_server_addresses
            self.client_addresses = original_client_addresses
            time.sleep(0.2)

    def prepare_workloads(self) -> None:
        entries = []
        cumulative_sum = 0
        for index in range(4):
            rng = random.Random(self.config.seed + index)
            digest = hashlib.sha256()
            total = 0
            for _ in range(self.config.entries_per_client):
                value = rng.randint(1, 100)
                digest.update(f"{value}\n".encode("ascii"))
                total += value
            cumulative_sum += total
            entries.append(
                {
                    "client_index": index,
                    "count": self.config.entries_per_client,
                    "sum": total,
                    "sha256": digest.hexdigest(),
                    "cumulative_grand_sum_after_client": cumulative_sum,
                }
            )
        self.manifest = {
            "seed": self.config.seed,
            "clients": 4,
            "entries_per_client": self.config.entries_per_client,
            "value_range": {"minimum": 1, "maximum": 100},
            "client_workloads": entries,
            "total_count": self.config.entries_per_client * 4,
            "total_sum": cumulative_sum,
        }
        self.assertion(
            "generated_workload_verified",
            True,
            {
                "entries_per_client": self.manifest["entries_per_client"],
                "total_count": self.manifest["total_count"],
                "total_sum": self.manifest["total_sum"],
            },
        )

    def run_lifecycle(self) -> None:
        self.start_relay("measured", self.relay_address)
        for server_id in INITIAL_SERVER_IDS:
            self.start_server(server_id)
        self.wait_topology(set(INITIAL_SERVER_IDS), 40, self.config.stage_timeout)
        for index in range(4):
            self.start_client(index)
        self.measured_started = time.monotonic()
        self.start_monitor()
        self.observed_client_counts.add(4)

        self.wait_progress(1)
        old = self.stop_client(0, graceful=True)

        self.wait_progress(2)
        self.stop_server(10, graceful=False)
        self.wait_topology({20, 30, 40}, 40, self.config.stage_timeout)

        self.start_client(0, resume=old, new_identity=True)
        new = self.client_status_wait(0)
        self.assertion(
            "graceful_restart_uses_new_identity",
            old is not None and old["client_id"] != new["client_id"],
            {"old": old and old["client_id"], "new": new["client_id"]},
        )
        self.observed_client_counts.add(4)

        self.wait_progress(3)
        pending = self.wait_client_pending(1)
        self.stop_client(1, graceful=False)
        crashed_state = pending
        self.start_client(
            1,
            resume=crashed_state,
            startup_hold=0.75,
        )
        resumed = self.client_status_wait(1)
        self.assertion(
            "abrupt_restart_preserves_pending_request",
            crashed_state is not None
            and crashed_state["pending"] is not None
            and resumed["client_id"] == crashed_state["client_id"]
            and resumed["pending"] == crashed_state["pending"],
            {
                "observed_before_kill": pending,
                "state_used_for_restart": crashed_state,
                "restarted": resumed,
            },
        )

        self.wait_progress(4)
        self.start_server(10)
        self.wait_topology({10, 20, 30, 40}, 40, self.config.stage_timeout)

        self.wait_progress(5)
        paused_two = self.stop_client(2, graceful=True)
        paused_three = self.stop_client(3, graceful=True)

        self.wait_progress(6)
        self.stop_server(40, graceful=False)
        self.wait_topology({10, 20, 30}, 30, self.config.stage_timeout)
        self.start_server(40)
        self.wait_topology({10, 20, 30, 40}, 40, self.config.stage_timeout)
        self.start_client(2, resume=paused_two, new_identity=True)
        self.observed_client_counts.add(3)
        self.start_client(3, resume=paused_three, new_identity=True)
        self.observed_client_counts.add(4)

        self.wait_progress(7)
        self.stop_server(10, graceful=True)
        self.wait_topology({20, 30, 40}, 40, self.config.stage_timeout)
        self.start_server(50)
        self.wait_topology({20, 30, 40, 50}, 50, self.config.stage_timeout)

        self.wait_progress(8)
        paused = {
            index: self.stop_client(index, graceful=True)
            for index in range(4)
        }
        self.assertion(
            "zero_active_clients_observed",
            not self.active_client_ids(),
            sorted(self.active_client_ids()),
        )
        time.sleep(self.config.zero_client_pause)
        for index in range(4):
            self.start_client(
                index,
                resume=paused[index],
                new_identity=True,
            )
            self.observed_client_counts.add(len(self.active_client_ids()))

        self.wait_progress(9)
        self.stop_server(20, graceful=False)
        self.wait_topology({30, 40, 50}, 50, self.config.stage_timeout)
        self.stop_server(30, graceful=True)
        self.wait_topology({40, 50}, 50, self.config.stage_timeout)
        self.stop_server(40, graceful=False)
        self.wait_topology({50}, 50, self.config.stage_timeout)

        self.wait_progress(10)
        self.start_server(20)
        self.wait_topology({20, 50}, 50, self.config.stage_timeout)
        self.start_server(60)
        self.wait_topology({20, 50, 60}, 60, self.config.stage_timeout)
        self.start_server(30)
        self.wait_topology({20, 30, 50, 60}, 60, self.config.stage_timeout)

        wait_until(
            lambda: all(
                (status := self.client_status(index)) is not None
                and status["completed"]
                for index in range(4)
            ),
            self.config.completion_timeout,
            "all workload clients",
        )
        wait_until(
            self.final_servers_match,
            self.config.stage_timeout,
            "final replicated state",
        )

    def client_status_wait(self, index: int) -> dict[str, Any]:
        result: dict[str, Any] | None = None

        def available() -> bool:
            nonlocal result
            result = self.client_status(index)
            return result is not None

        wait_until(available, self.config.stage_timeout, f"client {index} status")
        assert result is not None
        return result

    def wait_client_pending(self, index: int) -> dict[str, Any]:
        result: dict[str, Any] | None = None

        def pending() -> bool:
            nonlocal result
            result = self.client_status(index)
            return result is not None and result["pending"] is not None

        wait_until(pending, self.config.stage_timeout, f"client {index} pending")
        assert result is not None
        return result

    def final_servers_match(self) -> bool:
        expected_ids = {20, 30, 50, 60}
        statuses = self.statuses("server", expected_ids)
        if set(statuses) != expected_ids:
            return False
        expected_count = int(self.manifest["total_count"])
        expected_sum = int(self.manifest["total_sum"])
        signatures = {
            (
                status["num_reqs"],
                status["total_sum"],
                status["state_version"],
                status["clients_hash"],
            )
            for status in statuses.values()
        }
        return (
            len(signatures) == 1
            and all(
                status["num_reqs"] == expected_count
                and status["total_sum"] == expected_sum
                and status["state_version"] == expected_count
                for status in statuses.values()
            )
        )

    def verify(self) -> None:
        expected_count = int(self.manifest["total_count"])
        expected_sum = int(self.manifest["total_sum"])
        statuses = self.statuses("server", {20, 30, 50, 60})
        client_statuses = self.statuses("client", {0, 1, 2, 3})
        self.assertion(
            "all_workload_entries_acknowledged",
            sum(int(status["position"]) for status in client_statuses.values())
            == expected_count,
            {index: status.get("position") for index, status in client_statuses.items()},
        )
        self.assertion(
            "final_server_count_and_sum",
            all(
                status["num_reqs"] == expected_count
                and status["total_sum"] == expected_sum
                for status in statuses.values()
            ),
            statuses,
        )
        self.assertion(
            "final_server_versions_and_client_state_match",
            len(
                {
                    (status["state_version"], status["clients_hash"])
                    for status in statuses.values()
                }
            )
            == 1,
            statuses,
        )
        self.assertion(
            "all_server_counts_observed",
            {1, 2, 3, 4}.issubset(self.observed_server_counts),
            sorted(self.observed_server_counts),
        )
        self.assertion(
            "all_client_counts_observed",
            {0, 1, 2, 3, 4}.issubset(self.observed_client_counts),
            sorted(self.observed_client_counts),
        )
        pids = [record.pid for record in self.manager.records]
        self.assertion(
            "components_used_distinct_processes",
            len(pids) == len(set(pids)),
            pids,
        )

    def run(self) -> Path:
        status = "FAILED"
        error: str | None = None
        measured_duration: float | None = None
        try:
            self.profile_throughput = self.profile()
            self.prepare_workloads()
            self.projected_duration = int(self.manifest["total_count"]) / max(
                self.profile_throughput, 1.0
            )
            self.recorder.record(
                "duration_projection",
                throughput=self.profile_throughput,
                total_count=self.manifest["total_count"],
                projected_seconds=self.projected_duration,
                warning=self.projected_duration > 300,
            )
            config_payload = {
                **asdict(self.config),
                "output_root": str(self.config.output_root),
                "profile_throughput": self.profile_throughput,
                "projected_duration": self.projected_duration,
            }
            (self.run_dir / "config.json").write_text(
                json.dumps(config_payload, indent=2, sort_keys=True) + "\n"
            )
            started = time.monotonic()
            self.run_lifecycle()
            measured_duration = time.monotonic() - started
            self.verify()
            status = "PASSED"
            return self.run_dir
        except Exception as exc:
            error = repr(exc)
            self.recorder.record("benchmark_failed", error=error)
            raise
        finally:
            self.monitor_stop.set()
            if self.monitor_thread is not None:
                self.monitor_thread.join(timeout=2.0)
            final_server_statuses = self.statuses(
                "server", self.active_server_ids()
            )
            final_client_statuses = self.statuses(
                "client", self.active_client_ids()
            )
            self.manager.stop_all()
            self.status_client.close()
            processes = self.manager.records_payload()
            (self.run_dir / "processes.json").write_text(
                json.dumps(processes, indent=2, sort_keys=True) + "\n"
            )
            report = {
                "status": status,
                "error": error,
                "profile_throughput": self.profile_throughput,
                "projected_duration": self.projected_duration,
                "measured_duration": measured_duration,
                "duration_warning_recorded": self.warning_recorded,
                "manifest": self.manifest,
                "assertions": self.assertions,
                "observed_server_counts": sorted(self.observed_server_counts),
                "observed_client_counts": sorted(self.observed_client_counts),
                "final_server_statuses": final_server_statuses,
                "final_client_statuses": final_client_statuses,
                "process_count": len(processes),
            }
            (self.run_dir / "report.json").write_text(
                json.dumps(report, indent=2, sort_keys=True) + "\n"
            )
            self.recorder.record("benchmark_finished", status=status)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Independent-process UDP chaos benchmark"
    )
    parser.add_argument("--output-root", type=Path, default=Path("benchmark-results"))
    parser.add_argument("--seed", type=int, default=20260613)
    parser.add_argument("--entries", type=int, default=25_000)
    parser.add_argument("--target-duration", type=float, default=240.0)
    parser.add_argument("--base-port", type=int, default=47000)
    parser.add_argument("--calibration-requests", type=int, default=200)
    parser.add_argument("--stage-timeout", type=float, default=35.0)
    parser.add_argument("--completion-timeout", type=float, default=1200.0)
    parser.add_argument("--quick", action="store_true")
    args = parser.parse_args()
    config = ChaosConfig(
        output_root=args.output_root,
        seed=args.seed,
        entries_per_client=args.entries,
        target_duration=args.target_duration,
        base_port=args.base_port,
        calibration_requests_per_client=args.calibration_requests,
        stage_timeout=args.stage_timeout,
        completion_timeout=args.completion_timeout,
        sample_interval=0.5 if args.quick else 1.0,
        zero_client_pause=0.2 if args.quick else 2.0,
        warning_seconds=20.0 if args.quick else 270.0,
        quick=args.quick,
    )
    benchmark = ProcessChaosBenchmark(config)
    try:
        run_dir = benchmark.run()
    except Exception:
        print(f"FAILED report={benchmark.run_dir / 'report.json'}", flush=True)
        raise SystemExit(1)
    print(f"PASSED report={run_dir / 'report.json'}", flush=True)


if __name__ == "__main__":
    main()
