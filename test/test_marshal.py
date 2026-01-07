import unittest

from sinetstream.api import Message
import sinetstream.marshal

msgs = [(b'test message 001\x01', 0),
        (b'test message 002\x02', 1),
        (b'\x00\xff', 2)]

HDRLEN = 10  # = 2-bytes marker and 8-bytes fingerprint
TSLEN = 8   # position HDRLEN+TSLEN is the place at length of msg

class TestMarshal(unittest.TestCase):

    def _test_marshal(self, msg, ts):
        marshaller = sinetstream.marshal.Marshaller()
        bs = marshaller.marshal(msg, ts)

        unmarshaller = sinetstream.marshal.Unmarshaller()
        ts2, msg2 = unmarshaller.unmarshal(bs)
        self.assertEqual(msg2, msg)
        self.assertEqual(ts2, ts)

    def test_marshal(self):
        for msg, ts in msgs:
            self._test_marshal(msg, ts)

    def test_timestamp_us(self):
        us = 12345678
        msg = Message(None, None, us, None)
        self.assertEqual(msg.timestamp_us, us)
        self.assertEqual(msg.timestamp * 1000000, us)

    def test_header_size(self):
        self.assertEqual(len(sinetstream.marshal.avro_signle_object_format_marker), 2)
        self.assertEqual(len(sinetstream.marshal.message_schema_fingerprint), 8)



if __name__ == '__main__':
    unittest.main()
