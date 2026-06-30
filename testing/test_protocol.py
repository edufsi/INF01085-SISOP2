import unittest

from common.protocol import (
    MAX_UINT64,
    ProtocolError,
    assemble_snapshot,
    decode,
    encode,
    message,
    require_uint64,
    snapshot_chunks,
)


class ProtocolTests(unittest.TestCase):
    def test_json_round_trip(self) -> None:
        payload = message("CLIENT_REQUEST", client_id="abc", request_id=1, value=9)
        self.assertEqual(decode(encode(payload)), payload)

    def test_unsigned_64_bit_validation(self) -> None:
        self.assertEqual(require_uint64(MAX_UINT64, "value"), MAX_UINT64)
        for value in (-1, MAX_UINT64 + 1, True, "1"):
            with self.subTest(value=value):
                with self.assertRaises(ProtocolError):
                    require_uint64(value, "value")

    def test_snapshot_chunking_and_checksum(self) -> None:
        snapshot = {"clients": {str(i): {"last_req": i} for i in range(100)}}
        chunks = snapshot_chunks(snapshot, "transfer")
        rebuilt = assemble_snapshot(
            {int(chunk["index"]): str(chunk["data"]) for chunk in chunks},
            int(chunks[0]["count"]),
            str(chunks[0]["sha256"]),
        )
        self.assertEqual(rebuilt, snapshot)
        corrupted = {int(chunk["index"]): str(chunk["data"]) for chunk in chunks}
        corrupted[0] += "x"
        with self.assertRaises(ProtocolError):
            assemble_snapshot(
                corrupted, int(chunks[0]["count"]), str(chunks[0]["sha256"])
            )


if __name__ == "__main__":
    unittest.main()
