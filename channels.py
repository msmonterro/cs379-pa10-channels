"""A bounded FIFO channel for USILang heap handles."""

from collections import deque
import threading

from gc_heap import GCHeap


class Channel:
    CLOSED = object()

    def __init__(self, capacity: int, heap: GCHeap) -> None:
        if capacity <= 0:
            raise ValueError("Channel capacity must be positive")
        self._capacity = capacity
        self._heap = heap
        self._buffer = deque()
        self._closed = False
        lock = threading.Lock()
        self._not_empty = threading.Condition(lock)
        self._not_full = threading.Condition(lock)

    def put(self, value: int) -> None:
        with self._not_full:
            while len(self._buffer) == self._capacity and not self._closed:
                self._not_full.wait()
            if self._closed:
                raise RuntimeError("Cannot put into a closed channel")

            self._heap.incref(value)
            self._buffer.append(value)
            self._not_empty.notify()

    def get(self):
        with self._not_empty:
            while not self._buffer and not self._closed:
                self._not_empty.wait()
            if not self._buffer:
                return self.CLOSED

            handle = self._buffer.popleft()
            self._not_full.notify()
            return handle

    def close(self) -> None:
        with self._not_empty:
            self._closed = True
            self._not_empty.notify_all()
            self._not_full.notify_all()


def producer_consumer_driver(
    heap: GCHeap, values: list, channel: Channel, num_consumers: int = 1
):
    """Return the (handle, value) pairs processed by the consumers."""
    if num_consumers < 1:
        raise ValueError("At least one consumer is required")

    delivered = []
    delivered_lock = threading.Lock()
    heap_lock = threading.Lock()

    def produce():
        try:
            for value in values:
                with heap_lock:
                    handle = heap.allocate(value)
                try:
                    channel.put(handle)
                finally:
                    with heap_lock:
                        heap.decref(handle)
        finally:
            channel.close()

    def consume():
        while True:
            handle = channel.get()
            if handle is Channel.CLOSED:
                return
            try:
                with heap_lock:
                    value = heap._objects[handle]
                with delivered_lock:
                    delivered.append((handle, value))
            finally:
                with heap_lock:
                    heap.decref(handle)

    producer = threading.Thread(target=produce)
    consumers = [threading.Thread(target=consume) for _ in range(num_consumers)]
    for consumer in consumers:
        consumer.start()
    producer.start()
    producer.join()
    for consumer in consumers:
        consumer.join()
    return delivered
