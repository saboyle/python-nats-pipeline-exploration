#!/usr/bin/env python

""" p3_server.py: Example pipeline server (p3).

Note: Requires running NATS server configured on localhost:4222
      i.e. docker run -p 4222:4222 -p 8222:8222 -p 6222:6222 --name gnatsd -ti nats:latest
"""
__author__ = "Steve Boyle"
__version__ = "0.1.0"
__status__ = "Prototype"


import asyncio
import json
import uuid
import time
from nats.aio.client import Client as NATS

test_pauses = [10, 40, 10, 10]  # in milliseconds


def log(level, pipeline, stage, message, corr_id):
    print(f"{pipeline}, {stage}, {corr_id}, {message}")
    logging.info(f"{pipeline}, {stage}, {corr_id}, {message}")


def sync_null_transform(data, wait):
    time.sleep(wait / 1000)  # !!! Blocking wait
    return data

@asyncio.coroutine
def async_null_transform(data, wait):
    asyncio.sleep(wait / 1000)  # !!! Non-Blocking wait
    return data


def pipeline_p3(loop):
    #####################
    # Connection to NATS
    #####################
    nc = NATS()
    yield from nc.connect("localhost:4222", loop=loop)

    #####################
    # Message Handlers
    #####################
    async def mh_s1(msg):
        correlation_id = str(uuid.uuid4())
        log('info', 'p3', 's0', 'S1 initiated', correlation_id)

        jrequest = json.loads(msg.data.decode())

        # Inject a correlation id into the message to enable analysis across pipeline stages
        jresponse = {}
        jresponse['correlation_id'] = correlation_id
        jresponse['data'] = jrequest

        data = await async_null_transform(jresponse, test_pauses[0])  # Stage processing

        await nc.publish("p3.s1", json.dumps(data).encode('utf-8'))
        log('info', 'p3', 's1', 'S1 completed', correlation_id)

    async def mh_s2(msg):
        jrequest = json.loads(msg.data.decode())

        data = await async_null_transform(jrequest, test_pauses[1])  # Stage processing

        log('info', 'p3', 's2', 'S2 completed', data['correlation_id'])
        await nc.publish("p3.s2", msg.data)

    async def mh_s3(msg):
        jrequest = json.loads(msg.data.decode())

        data = await async_null_transform(jrequest, test_pauses[1])  # Stage processing

        log('info', 'p3', 's3', 'S3 completed', data['correlation_id'])
        await nc.publish("p3.s3", msg.data)

    async def mh_s4(msg):
        jrequest = json.loads(msg.data.decode())

        data = await async_null_transform(jrequest, test_pauses[1])  # Stage processing

        log('info', 'p3', 's4', 's4 completed', data['correlation_id'])

    ######################
    # Pipeline Creation
    ######################
    yield from nc.subscribe("p3.s0", cb=mh_s1, queue="p3.s0")
    # yield from nc.subscribe("p3.s1", cb=mh_s2)
    # yield from nc.subscribe("p3.s2", cb=mh_s3)
    # yield from nc.subscribe("p3.s3", cb=mh_s4)


if __name__ == '__main__':
    import logging

    logging.basicConfig(filename='../logs/p3_pipeline.log', filemode='a', format='%(asctime)s, %(message)s',
                        level=logging.DEBUG)
    logger = logging.getLogger(__name__)
    logging.getLogger("asyncio").setLevel(logging.WARNING)

    event_loop = asyncio.get_event_loop()
    event_loop.run_until_complete(pipeline_p3(event_loop))

    try:
        print('S1 Worker listening')
        event_loop.run_forever()
    finally:
        print('Closing')
