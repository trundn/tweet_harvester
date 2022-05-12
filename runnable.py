# Useful in thread-safe get and set boolean value
from atomic_boolean import AtomicBoolean

class Runnable(object):
    def __init__(self):
        self.handled_thread_name = ""
        self.is_cancelled = AtomicBoolean(False)

    def run(self):
        raise NotImplementedError( "Should have implemented this.")

    def cancel(self):
        self.is_cancelled.set(True)