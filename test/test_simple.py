import unittest
import random
import sinetstream

def gen_topic():
    return f"topic-{random.randint(0, 999999)}"

SERVICE = "raspipicow"

def test_reader():
    topic = gen_topic()
    with sinetstream.MessageReader(SERVICE, topic) as _:
        pass


def test_writer():
    topic = gen_topic()
    with sinetstream.MessageWriter(SERVICE, topic) as _:
        pass


def test_ping_writer():
    topic = gen_topic()
    with sinetstream.MessageWriter(SERVICE, topic) as wr:
        wr._plugin.pingpong()
        wr._plugin.pingpong()
        wr._plugin.pingpong()


def test_ping_reader():
    topic = gen_topic()
    with sinetstream.MessageReader(SERVICE, topic) as rd:
        rd._plugin.pingpong()
        rd._plugin.pingpong()
        rd._plugin.pingpong()


def test_pubsub():
    topic = gen_topic()
    with sinetstream.MessageReader(SERVICE, topic) as rd:
        with sinetstream.MessageWriter(SERVICE, topic) as wr:
            m1 = f"msg-{random.randint(0, 999999)}"
            rd._plugin.ping()
            wr.publish(m1)

            it = iter(rd)
            m2 = next(it)
            assert m2.value == m1


if __name__ == '__main__':
    unittest.main()
