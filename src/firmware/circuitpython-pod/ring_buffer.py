"""Small fixed-size ring buffer for recent telemetry samples."""


class RingBuffer:
    def __init__(self, capacity):
        if capacity <= 0:
            raise ValueError("capacity must be > 0")
        self._capacity = capacity
        self._items = [None] * capacity
        self._count = 0
        self._next_index = 0

    @property
    def capacity(self):
        return self._capacity

    def __len__(self):
        return self._count

    def append(self, item):
        self._items[self._next_index] = item
        self._next_index = (self._next_index + 1) % self._capacity
        if self._count < self._capacity:
            self._count += 1

    def latest(self):
        if self._count == 0:
            return None
        return self._items[(self._next_index - 1) % self._capacity]

    def to_list(self):
        # Return samples oldest-to-newest, which is the most useful order for
        # future resend logic and for manual inspection during debugging.
        ordered = []
        start = (self._next_index - self._count) % self._capacity
        for offset in range(self._count):
            ordered.append(self._items[(start + offset) % self._capacity])
        return ordered

    def iter_from_seq(self, start_seq):
        for item in self.to_list():
            if item["seq"] >= start_seq:
                yield item
