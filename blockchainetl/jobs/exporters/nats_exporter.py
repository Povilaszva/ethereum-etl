import collections
import json
import logging
import os
import asyncio
import nats
import concurrent.futures

from blockchainetl.jobs.exporters.converters.composite_item_converter import CompositeItemConverter


class NatsItemExporter:

    def __init__(self, output, item_type_to_subject_mapping, converters=()):
        self.item_type_to_subject_mapping = item_type_to_subject_mapping
        self.converter = CompositeItemConverter(converters)
        self.connection_url = self.get_connection_url(output)
        self.credentials_path = os.getenv("NATS_CREDENTIALS")
        self.topic_prefix = os.getenv("NATS_SUBJECT_PREFIX")
        if not self.credentials_path:
            raise ValueError("NATS_CREDENTIALS environment variable not set!")
        self.nc = nats.aio.client.Client()
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        self.open()

    def __del__(self):
        self.close()
        self.executor.shutdown()

    def get_connection_url(self, output):
        try:
            return output.split('//')[1]
        except KeyError:
            raise Exception('Invalid nats output param, It should be in format of "nats://127.0.0.1:4222"')

    def open(self):
        loop = asyncio.get_event_loop()
        loop.run_in_executor(self.executor, self._async_open)

    async def _async_open(self):
        await self.nc.connect(self.connection_url, user_credentials=self.credentials_path)

    def export_items(self, items):
        for item in items:
            self.export_item(item)

    def export_item(self, item):
        item_type = item.get('type')
        if item_type is not None and item_type in self.item_type_to_subject_mapping:
            data = json.dumps(item).encode('utf-8')
            logging.debug(data)
            topic = self.topic_prefix + self.item_type_to_subject_mapping[item_type]
            loop = asyncio.get_event_loop()
            loop.run_in_executor(self.executor, self._async_publish, topic, data)
        else:
            logging.warning('Subject for item type "{}" is not configured.'.format(item_type))

    async def _async_publish(self, topic, data):
        await self.nc.publish(topic, data)

    def convert_items(self, items):
        for item in items:
            yield self.converter.convert_item(item)

    def close(self):
        loop = asyncio.get_event_loop()
        loop.run_in_executor(self.executor, self._async_close)

    async def _async_close(self):
        await self.nc.close()


def group_by_item_type(items):
    result = collections.defaultdict(list)
    for item in items:
        result[item.get('type')].append(item)
    return result
