from abc import ABC, abstractmethod


class QueueContract(ABC):

    @abstractmethod
    def push(self):
        pass

    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def consume(self, channel, fair=False):
        pass

    @abstractmethod
    def work(self):
        pass

    @abstractmethod
    def run_failed_jobs(self):
        pass

    @abstractmethod
    def add_to_failed_queue_table(self):
        pass
