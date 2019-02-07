""" A QueueWorkCommand Command """

import inspect
import pickle

from cleo import Command

from config import queue
from masonite.exceptions import DriverLibraryNotFound
from masonite import Queue
from masonite.helpers.filesystem import copy_migration



class QueueTableCommand(Command):
    """
    Start the queue worker

    queue:table
    """

    def handle(self):
        copy_migration('masonite/snippets/migrations/create_failed_jobs_table.py')
        self.info('Migration created successfully')
