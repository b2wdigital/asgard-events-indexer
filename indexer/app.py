import asyncio

from indexer.conf import settings
from indexer.connection import HTTPConnection
from indexer.mesos.events.consumer import MesosEventConsumer


async def main():
    consumer = MesosEventConsumer(
        HTTPConnection(urls=settings.MESOS_MASTER_URLS)
    )
    await consumer.start()
