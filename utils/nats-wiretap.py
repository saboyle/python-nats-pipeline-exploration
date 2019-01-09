import sys
import asyncio
import datetime
import signal

from nats.aio.client import Client as NATS

def wire_tap(msg):
    ts = datetime.datetime.now()
    print(ts.strftime("%Y-%m-%d %H:%M:%S.%f"), msg.subject, msg.reply, msg.data.decode(), msg.reply)

def main(loop, subject):
    #####################
    # Connection to NATS
    #####################
    nc = NATS()
    yield from nc.connect("localhost:4222", loop=loop)

    #####################
    # Message Handlers
    #####################
    async def mh_s1(msg):
        await wire_tap(msg)

    yield from nc.subscribe(subject, cb=wire_tap)


def signal_handler(sig, frame):
    sys.exit(0)


if __name__ == "__main__":
    if len(sys.argv) == 2:
        event_loop = asyncio.get_event_loop()
        event_loop.run_until_complete(main(event_loop, subject=sys.argv[1]))

        signal.signal(signal.SIGINT, signal_handler)

        print('Wiretap enabled: Listening to ', sys.argv[1])
        event_loop.run_forever()
