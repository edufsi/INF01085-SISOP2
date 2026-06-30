import json
from pathlib import Path
import socket
import tempfile
import time
import unittest

from benchmarks.chaos import ChaosConfig, ProcessChaosBenchmark
from benchmarks.relay import DiscoveryRelay
from common.protocol import decode, encode, message
from benchmarks.workload import generate_workloads, load_and_verify_workloads


def find_base_port() -> int:
    for base in range(50000, 54000, 400):
        sockets: list[socket.socket] = []
        try:
            for host, port in [
                ("127.0.0.10", base),
                ("127.0.0.20", base),
                ("127.0.0.1", base + 100),
                ("127.0.0.10", base + 200),
                ("127.0.0.20", base + 200),
                ("127.0.0.1", base + 300),
            ]:
                sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                sock.bind((host, port))
                sockets.append(sock)
            return base
        except OSError:
            pass
        finally:
            for sock in sockets:
                sock.close()
    raise RuntimeError("no free UDP port range for benchmark test")


class BenchmarkTests(unittest.TestCase):
    def test_workload_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            generated = generate_workloads(
                output,
                entries_per_client=25,
                seed=1234,
                clients=4,
                minimum=1,
                maximum=100,
            )
            loaded, workloads = load_and_verify_workloads(output / "manifest.json")
            self.assertEqual(loaded["total_count"], 100)
            self.assertEqual(loaded["total_sum"], sum(map(sum, workloads)))
            self.assertEqual(generated["total_sum"], loaded["total_sum"])
            self.assertEqual(
                loaded["client_workloads"][-1]["cumulative_grand_sum_after_client"],
                loaded["total_sum"],
            )

    def test_relay_registration_expiry_and_forwarding(self) -> None:
        relay = DiscoveryRelay(("127.0.0.1", 0), expiry=0.02)
        server = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        client = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        server.bind(("127.0.0.1", 0))
        server.settimeout(0.2)
        client.bind(("127.0.0.1", 0))
        try:
            relay.handle(
                message(
                    "RELAY_REGISTER",
                    server_id=10,
                    host=server.getsockname()[0],
                    port=server.getsockname()[1],
                ),
                server.getsockname(),
            )
            discovery = message("CLIENT_DISCOVERY", client_id="client-a")
            relay.handle(discovery, client.getsockname())
            forwarded, _ = server.recvfrom(1200)
            self.assertEqual(decode(forwarded), discovery)
            heartbeat = message(
                "BACKUP_HEARTBEAT",
                server_id=10,
                host=server.getsockname()[0],
                port=server.getsockname()[1],
                term=1,
                leader_id=20,
                state_version=0,
            )
            relay.handle(heartbeat, server.getsockname())
            forwarded, _ = server.recvfrom(1200)
            self.assertEqual(decode(forwarded), heartbeat)
            time.sleep(0.03)
            self.assertEqual(relay.active_servers(), [])
        finally:
            relay.socket.close()
            server.close()
            client.close()

    def test_compressed_full_lifecycle(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            benchmark = ProcessChaosBenchmark(
                ChaosConfig(
                    output_root=Path(directory),
                    seed=4321,
                    entries_per_client=2500,
                    target_duration=45.0,
                    base_port=find_base_port(),
                    calibration_requests_per_client=20,
                    stage_timeout=35.0,
                    completion_timeout=180.0,
                    sample_interval=0.5,
                    zero_client_pause=0.05,
                    warning_seconds=20.0,
                    quick=True,
                )
            )
            run_dir = benchmark.run()
            report = json.loads((run_dir / "report.json").read_text(encoding="utf-8"))
            self.assertEqual(report["status"], "PASSED")
            self.assertEqual(report["observed_server_counts"], [1, 2, 3, 4])
            self.assertEqual(report["observed_client_counts"], [0, 1, 2, 3, 4])
            processes = json.loads(
                (run_dir / "processes.json").read_text(encoding="utf-8")
            )
            self.assertEqual(
                len({process["pid"] for process in processes}),
                len(processes),
            )


if __name__ == "__main__":
    unittest.main()
