# Constructs higher-level threading interfaces on top of the lower level _thread module
import threading
# Import module sys to get the type of exception
import sys
# Provides a standard interface to extract, format and print stack traces 
import traceback
# Useful in threaded programming when information must be exchanged safely between multiple threads
from queue import *
# Provides a common protocol for objects that wish to execute code while they are active
from runnable import Runnable
# Useful in thread-safe get and set boolean value
from atomic_boolean import AtomicBoolean

class Worker(threading.Thread):
    def __init__(self, is_stopped, queue):
        super(Worker, self).__init__()
        self.queue = queue
        self.daemon = True
        self.is_stopped = is_stopped
        self.handling_job_list = []

    def run(self):
        try:
            while(not self.is_stopped.get()):
                job = self.queue.get()
                if ((job is not None) and (isinstance(job, Runnable))):
                    self.handling_job_list.append(job)
                    job.handled_thread_name = self.name
                    job.run()
        except:
            print("Exception", sys.exc_info()[0], "occurred.")
            traceback.print_exc(file = sys.stdout)
