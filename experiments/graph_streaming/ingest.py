import asyncio
import re
from threading import Thread, Lock
from typing import List
import time

import ray

from common import FEED_SIZE, Event


@ray.remote
class IngestActor:
    def __init__(self, data_source: str, event_interval: float):
        # timestamp: int of microseconds
        self._events: List[Event] = []
        self._old_events = []
        self._latest_timestamp_seen: int = 0

        self._data_source = data_source
        self._event_interval = event_interval

        self._thread = None
        self._lock = Lock()

    def start_ingest(self, start_index: int):
        if self._thread is not None:
            return
        self._thread = Thread(target=self._ingest, args=(start_index,))
        self._thread.start()

    def _ingest(self, start_index: int):
        # TODO: Use file byte index for start_index instead of num rows.
        # TODO: f.seek before starting to read.
        with open(self._data_source) as f:
            while True:
                f.seek(start_index)
                data = f.read(FEED_SIZE)
                if not data:
                    break
                start_index = f.tell()
                time.sleep(self._event_interval)

                # drop the unfinished end
                for i, c in enumerate(reversed(data)):
                    if i == "\n":
                        if i > 0:
                            start_index -= i
                            f.seek(start_index)
                            data = data[:-i]
                        break
                # parse the data
                for line in data.split("\n"):
                    self.process(line, start_index)

    def process(self, line: str, start_index: int):
        with self._lock:
            pattern = re.compile("([0-9]+) ([0-9]+)")
            r = pattern.match(line)
            if r is not None:
                event = Event(
                    time.time_ns(), int(r.group(1)), int(r.group(2)), start_index
                )
                self._latest_timestamp_seen = max(
                    self._latest_timestamp_seen, event.timestamp
                )
                self._events.append(event)

    async def get_current_batch(self):
        return [(event.a, event.b) for event in self._events]

    @ray.method(num_returns=2)
    async def get_next_batch(self, timestamp: int):
        while self._latest_timestamp_seen == 0 or not self._events:
            await asyncio.sleep(0.1)
        while self._latest_timestamp_seen < timestamp:
            await asyncio.sleep((timestamp - self._latest_timestamp_seen) / 1e9)
        with self._lock:
            # Only return updates up to the given timestamp.
            future_events = []
            event_batch = []
            start_index = 0

            while self._events:
                event = self._events.pop()
                if event.timestamp <= timestamp:
                    event_batch.append((event.a, event.b))
                    start_index = max(start_index, event.start_index)
                else:
                    future_events.append(event)
            self._events = future_events[::-1]

        # Take the last input index and save it with the output. We'll use this
        # to recover the ingest actor if it fails.
        return event_batch[::-1], start_index


@ray.remote
def create_ingest_actors(n_ingestion: int, n_event_interval: int):
    actors = []
    for i in range(n_ingestion):
        actors.append(IngestActor.remote(
            f"/exoflow/twitter_dataset/chunk_{i}.txt", n_event_interval))
    return actors
