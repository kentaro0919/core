"""Async Driver Method."""

import threading
import inspect

from masonite.contracts import QueueContract
from masonite.drivers import BaseQueueDriver
from masonite.app import App
from masonite.helpers import HasColoredCommands


class QueueAsyncDriver(QueueContract, BaseQueueDriver, HasColoredCommands):
    """Queue Aysnc Driver."""

    def __init__(self, app: App):
        """Queue Async Driver.

        Arguments:
            Container {masonite.app.App} -- The application container.
        """
        self.container = app

    def push(self, *objects, args=(), callback='handle', ran=1, channel=None):
        """Push objects onto the async stack.

        Arguments:
            objects {*args of objects} - This can be several objects as parameters into this method.
        """
        for obj in objects:
            if inspect.isclass(obj):
                obj = self.container.resolve(obj)

            try:
                thread = threading.Thread(
                    target=getattr(obj, callback), args=args, kwargs={})
            except AttributeError:
                # Could be wanting to call only a method asyncronously
                thread = threading.Thread(
                    target=obj, args=args, kwargs={})

            thread.start()

    def connect(self):
        return self
    
    def consume(self, channel, fair=False):
        raise NotImplementedError('The async driver does not implement consume')
    
    def work(self):
        raise NotImplementedError('The async driver does not implement work')
