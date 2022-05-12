# Constructs higher-level threading interfaces on top of the lower level _thread module
import threading

class AtomicInteger(object):
    def __init__(self, initial = 0):
        self.value = initial
        self.lock = threading.Lock()

    def get(self):
        with self.lock:
            return self.value

    def increment(self, num = 1):
        with self.lock:
            self.value += num
            return self.value

    def decrement(self, num = 1):
        with self.lock:
            self.value -= num
            return self.value