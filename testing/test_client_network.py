import errno
from queue import Empty, Queue
import unittest
from unittest.mock import patch

from client.discovery import descobrir_servidor
from client.interface import descobrir_com_retry
from client.processing import enviar_valor_stop_and_wait
from common.protocol import decode, encode, message


class DiscoverySocket:
    def __init__(self) -> None:
        self.send_attempts = 0
        self.timeouts: list[float] = []

    def sendto(self, data: bytes, address: tuple[str, int]) -> int:
        self.send_attempts += 1
        if self.send_attempts == 1:
            raise OSError(errno.ENETUNREACH, "Network is unreachable")
        self.last_discovery = decode(data)
        self.last_address = address
        return len(data)

    def settimeout(self, timeout: float) -> None:
        self.timeouts.append(timeout)

    def recvfrom(self, _size: int) -> tuple[bytes, tuple[str, int]]:
        return (
            encode(
                message(
                    "LEADER",
                    server_id=1,
                    host="10.0.0.5",
                    port=45000,
                    leader_id=1,
                    state_version=0,
                )
            ),
            ("10.0.0.5", 45000),
        )


class RequestSocket:
    def __init__(self) -> None:
        self.send_attempts = 0
        self.successful_sends: list[tuple[dict, tuple[str, int]]] = []
        self.timeouts: list[float] = []

    def sendto(self, data: bytes, address: tuple[str, int]) -> int:
        self.send_attempts += 1
        if self.send_attempts == 1:
            raise OSError(errno.ENETUNREACH, "Network is unreachable")
        self.successful_sends.append((decode(data), address))
        return len(data)

    def settimeout(self, timeout: float) -> None:
        self.timeouts.append(timeout)

    def recvfrom(self, _size: int) -> tuple[bytes, tuple[str, int]]:
        return (
            encode(
                message(
                    "CLIENT_ACK",
                    client_id="client-a",
                    request_id=12,
                    num_reqs=34,
                    total_sum=987,
                )
            ),
            ("10.0.0.9", 45000),
        )


class BrokenRequestSocket:
    def sendto(self, _data: bytes, _address: tuple[str, int]) -> int:
        raise OSError(errno.EBADF, "Bad file descriptor")


class ClientNetworkRecoveryTests(unittest.TestCase):
    def drain_queue(self, queue: Queue) -> list[str]:
        messages = []
        while True:
            try:
                messages.append(queue.get_nowait())
            except Empty:
                return messages

    def test_discovery_retries_when_network_is_unreachable(self) -> None:
        output_queue: Queue = Queue()
        socket = DiscoverySocket()

        with patch("client.interface.time.sleep", return_value=None):
            leader = descobrir_com_retry(socket, 45000, "client-a", output_queue)

        self.assertEqual(leader, ("10.0.0.5", 45000))
        self.assertEqual(socket.send_attempts, 2)
        self.assertEqual(socket.last_discovery["type"], "CLIENT_DISCOVERY")
        messages = self.drain_queue(output_queue)
        self.assertTrue(any("Rede indisponível" in item for item in messages))
        self.assertTrue(any("server_addr 10.0.0.5" in item for item in messages))

    def test_direct_discovery_prefers_packet_source_over_stale_advertised_host(self) -> None:
        class StaleHostDiscoverySocket:
            def sendto(self, data: bytes, address: tuple[str, int]) -> int:
                self.last_discovery = decode(data)
                self.last_address = address
                return len(data)

            def settimeout(self, _timeout: float) -> None:
                pass

            def recvfrom(self, _size: int) -> tuple[bytes, tuple[str, int]]:
                return (
                    encode(
                        message(
                            "LEADER",
                            server_id=1,
                            host="10.0.0.5",
                            port=45000,
                            leader_id=1,
                            state_version=0,
                        )
                    ),
                    ("10.0.0.99", 45000),
                )

        leader, _ = descobrir_servidor(
            StaleHostDiscoverySocket(),
            45000,
            "client-a",
        )

        self.assertEqual(leader, ("10.0.0.99", 45000))

    def test_relay_discovery_keeps_advertised_server_endpoint(self) -> None:
        class RelayDiscoverySocket:
            def sendto(self, data: bytes, address: tuple[str, int]) -> int:
                self.last_discovery = decode(data)
                self.last_address = address
                return len(data)

            def settimeout(self, _timeout: float) -> None:
                pass

            def recvfrom(self, _size: int) -> tuple[bytes, tuple[str, int]]:
                return (
                    encode(
                        message(
                            "LEADER",
                            server_id=1,
                            host="10.0.0.5",
                            port=45000,
                            leader_id=1,
                            state_version=0,
                        )
                    ),
                    ("127.0.0.1", 50300),
                )

        leader, _ = descobrir_servidor(
            RelayDiscoverySocket(),
            45000,
            "client-a",
            discovery_address=("127.0.0.1", 50300),
        )

        self.assertEqual(leader, ("10.0.0.5", 45000))

    def test_request_send_retries_same_pending_request_after_rediscovery(self) -> None:
        output_queue: Queue = Queue()
        socket = RequestSocket()
        rediscover_calls = 0

        def rediscover() -> tuple[str, int]:
            nonlocal rediscover_calls
            rediscover_calls += 1
            return ("10.0.0.9", 45000)

        num_reqs, total_sum, leader = enviar_valor_stop_and_wait(
            socket,
            ("10.0.0.1", 45000),
            "client-a",
            12,
            55,
            output_queue,
            rediscover,
        )

        self.assertEqual((num_reqs, total_sum, leader), (34, 987, ("10.0.0.9", 45000)))
        self.assertEqual(rediscover_calls, 1)
        self.assertEqual(socket.send_attempts, 2)
        self.assertEqual(len(socket.successful_sends), 1)
        payload, address = socket.successful_sends[0]
        self.assertEqual(address, ("10.0.0.9", 45000))
        self.assertEqual(payload["type"], "CLIENT_REQUEST")
        self.assertEqual(payload["client_id"], "client-a")
        self.assertEqual(payload["request_id"], 12)
        self.assertEqual(payload["value"], 55)

    def test_request_rediscover_after_repeated_retry_responses(self) -> None:
        class RetryThenAckSocket:
            def __init__(self) -> None:
                self.sent: list[tuple[dict, tuple[str, int]]] = []
                self.responses = 0

            def sendto(self, data: bytes, address: tuple[str, int]) -> int:
                self.sent.append((decode(data), address))
                return len(data)

            def settimeout(self, _timeout: float) -> None:
                pass

            def recvfrom(self, _size: int) -> tuple[bytes, tuple[str, int]]:
                self.responses += 1
                if self.responses <= 3:
                    return (
                        encode(message("RETRY", reason="state_gap")),
                        ("10.0.0.1", 45000),
                    )
                return (
                    encode(
                        message(
                            "CLIENT_ACK",
                            client_id="client-a",
                            request_id=12,
                            num_reqs=34,
                            total_sum=987,
                        )
                    ),
                    ("10.0.0.9", 45000),
                )

        output_queue: Queue = Queue()
        socket = RetryThenAckSocket()
        rediscover_calls = 0

        def rediscover() -> tuple[str, int]:
            nonlocal rediscover_calls
            rediscover_calls += 1
            return ("10.0.0.9", 45000)

        with patch("client.processing.time.sleep", return_value=None):
            num_reqs, total_sum, leader = enviar_valor_stop_and_wait(
                socket,
                ("10.0.0.1", 45000),
                "client-a",
                12,
                55,
                output_queue,
                rediscover,
            )

        self.assertEqual((num_reqs, total_sum, leader), (34, 987, ("10.0.0.9", 45000)))
        self.assertEqual(rediscover_calls, 1)
        self.assertEqual([address for _payload, address in socket.sent], [
            ("10.0.0.1", 45000),
            ("10.0.0.1", 45000),
            ("10.0.0.1", 45000),
            ("10.0.0.9", 45000),
        ])
        for payload, _address in socket.sent:
            self.assertEqual(payload["request_id"], 12)
            self.assertEqual(payload["value"], 55)

    def test_request_send_reraises_non_transient_os_errors(self) -> None:
        output_queue: Queue = Queue()
        rediscover_calls = 0

        def rediscover() -> tuple[str, int]:
            nonlocal rediscover_calls
            rediscover_calls += 1
            return ("10.0.0.9", 45000)

        with self.assertRaises(OSError) as error:
            enviar_valor_stop_and_wait(
                BrokenRequestSocket(),
                ("10.0.0.1", 45000),
                "client-a",
                12,
                55,
                output_queue,
                rediscover,
            )

        self.assertEqual(error.exception.errno, errno.EBADF)
        self.assertEqual(rediscover_calls, 0)


if __name__ == "__main__":
    unittest.main()
