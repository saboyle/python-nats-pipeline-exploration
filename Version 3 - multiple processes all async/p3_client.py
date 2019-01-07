import asyncio
import json

from nats.aio.client import Client as NATS

NUM_MESSAGES = 1000


def pub_random(loop):
    nc = NATS()
    yield from nc.connect("localhost:4222", loop=loop)

    if nc.last_error:
        print("ERROR received from NATS: ", nc.last_error)
    else:
        print('Submitting random requests')
        for i in range(NUM_MESSAGES):
            jdata = {"i": i}
            yield from nc.publish("p3.s0", json.dumps(jdata).encode('utf-8'))


if __name__ == '__main__':
    event_loop = asyncio.get_event_loop()
    event_loop.run_until_complete(pub_random(event_loop))



