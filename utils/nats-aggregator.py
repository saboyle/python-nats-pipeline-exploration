import sys
import asyncio
import time
import signal
import json

from nats.aio.client import Client as NATS


cache = {}


def aggregator(loop, subject):
    #####################
    # Connection to NATS
    #####################
    nc = NATS()
    yield from nc.connect("localhost:4222", loop=loop)

    ################################################################################
    # Aggregate stage timestamps into a single dict and publish summary to new queue
    ################################################################################
    async def aggregate(msg):
        jmsg = json.loads(msg.data.decode())
        subject = msg.subject

        if subject == "p1.s0":
            pass
        else:
            correlation_id = jmsg['correlation_id']

            ts = time.time()

            if correlation_id in cache:
                # print("Correlation Id in cache")

                if subject in cache[correlation_id]:
                    print('Error - loop detected')
                else:
                    cache[correlation_id][subject] = ts

                    # Check if all stages in the pipeline are complete and publish to a control queue.
                    if sorted(cache[correlation_id].keys()) == ['p1.s1', 'p1.s2', 'p1.s3']:
                        summary = cache[correlation_id]
                        summary['correlation_id'] = correlation_id

                        await nc.publish('c1.monitor', json.dumps(summary).encode('utf-8'))

                        print("Completed: ", correlation_id, cache[correlation_id])

                        # Remove the completed trace from the cache
                        del cache[correlation_id]
                        await nc.publish('c1.inflight', json.dumps({'inflight': len(cache)}).encode('utf-8'))
            else:
                # print("NEW Correlation Id")
                cache[correlation_id] = {}
                cache[correlation_id][subject] = ts
                # print(cache)

    #####################
    # Message Handlers
    #####################
    async def mh_s1(msg):
        await aggregate(msg)

    yield from nc.subscribe(subject, cb=mh_s1)


def signal_handler(sig, frame):
    sys.exit(0)


if __name__ == "__main__":

    event_loop = asyncio.get_event_loop()
    event_loop.run_until_complete(aggregator(event_loop, subject="p1.*"))

    signal.signal(signal.SIGINT, signal_handler)

    print('Aggregator enabled: Listening to ', "p1.*")
    event_loop.run_forever()
