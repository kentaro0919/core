""" A QueueFailedCommand Command """

from cleo import Command

from masonite.exceptions import DriverLibraryNotFound
from masonite import Queue


class QueueFailedCommand(Command):
    """
    Run all failed jobs

    queue:failed
        {--d|driver=default : Specify the driver you would like to connect to}
    """

    def handle(self):
        from wsgi import container

        if self.option('driver') == 'default':
            queue = container.make(Queue)
        else:
            queue = container.make(Queue).driver(self.option('driver'))

        queue.run_failed_jobs()
