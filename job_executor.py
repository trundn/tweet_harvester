# Provides various time-related functions
import time
# Useful in threaded programming when information must be exchanged safely between multiple threads
from queue import *
# Provide a common protocol for objects that wish to execute code while they are active
from runnable import Runnable
# Reponsible for executing queued runnable jobs
from worker import Worker
# Useful in thread-safe increase and decrease integer value
from atomic_integer import AtomicInteger
# Useful in thread-safe get and set boolean value
from atomic_boolean import AtomicBoolean
# The harvest constant definitions
import constants

class JobExecutor(object):
    def __init__(self, min_thread_count, max_thread_count, max_queue_size):
        self.min_thread_count = min_thread_count
        self.max_thread_count = max_thread_count
        self.max_queue_size = max_queue_size

        self.thread_list = []
        self.job_queue = Queue(self.max_queue_size)
        self.is_stopped = AtomicBoolean(False)
        self.current_thread_count = AtomicInteger(0)

        if (self.min_thread_count > 0):
            for i in range(self.min_thread_count):
                # Instantiate the worker thread
                worker = self.add_thread()
                # Start the worker thread
                worker.start()

    def set_thread_name(self, index, thread_name):
        if (thread_name):
            if 0 <= index < len(self.thread_list):
                self.thread_list[index].name = thread_name

    def add_thread(self):
        # Instantiate the worker thread
        worker = Worker(self.is_stopped, self.job_queue)

        # Increase the current thread coun
        self.current_thread_count.increment()
        self.thread_list.append(worker)

        return worker

    def add_thread_if_under_max(self):
        if ((self.max_thread_count == -1) or (self.current_thread_count.get() < self.max_thread_count)):
            worker = self.add_thread()
            worker.start()

    def queue(self, runnable):
        if (isinstance(runnable, Runnable)):
            if (not self.is_stopped.get()):
                self.job_queue.put(runnable)
                self.add_thread_if_under_max()
            else:
                raise RuntimeError("Thread pool job executor is being terminated. Cannot queue a new runnable task.")
        else:
            raise RuntimeError("The to be queued job should be Runnable instance.")
    
    def terminate(self):
        self.job_queue.clear()
        self.stop()

    def stop(self):
        self.is_stopped.set(True)
        for worker in self.thread_list:
            for job in worker.handling_job_list:
                if (isinstance(job, Runnable)):
                    runnable = Runnable(job)
                    runnable.cancel()

    def current_time_millis(self):
        return int(round(time.time() * 1000))

    def force_interrupt(self):
        for thread in self.thread_list:
            thread.kill()

    def is_any_thread_alive(self):
        result = False

        # Check thread alive statuses
        for thread in self.thread_list:
            if (thread.is_alive() is True):
                result = True
                break

        return result

    def wait_for_termination(self, timeout):
        if (not self.is_stopped.get()):
            raise RuntimeError("Thread pool job executor is not terminated before waiting for termination.")

        start_time = self.current_time_millis()
        while(self.current_time_millis() - start_time <= timeout):
            is_all_stopped = True

            # Check thread alive statuses
            for thread in self.thread_list:
                if (thread.is_alive() is True):
                    is_all_stopped = False
                    break

            # All threads are stopped
            if (is_all_stopped):
                return

            # Sleep 1 second before checking thread alive status again
            time.sleep(constants.ONE_SECOND)

        raise RuntimeError("Unable to terminate the thread pool job exectuor within the specified timeout [{}] ms.".format(timeout))
