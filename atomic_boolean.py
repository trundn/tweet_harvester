# Constructs higher-level threading interfaces on top of the lower level _thread module
import threading

class AtomicBoolean(object):
    def __init__(self, initial = False):
        self.value = initial
        self.lock = threading.Lock()

    def set(self, bool = True):
        with self.lock:
            self.value = bool
            return self.value

    def get(self):
        with self.lock:
            return self.value