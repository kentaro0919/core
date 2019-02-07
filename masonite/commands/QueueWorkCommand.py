""" A QueueWorkCommand Command """

import inspect
import pickle

from cleo import Command

from config import queue
from masonite.exceptions import DriverLibraryNotFound
from masonite import Queue


class QueueWorkCommand(Command):
    """
    Start the queue worker

    queue:work
        {--c|channel=default : The channel to listen on the queue}
        {--d|driver=default : Specify the driver you would like to connect to}
        {--f|fair : Send jobs to queues that have no jobs instead of randomly selecting a queue}
    """

    def handle(self):
        from wsgi import container

        if self.option('driver') == 'default':
            queue = container.make(Queue)
        else:
            queue = container.make(Queue).driver(self.option('driver'))

        queue.connect().consume(self.option('channel'))
