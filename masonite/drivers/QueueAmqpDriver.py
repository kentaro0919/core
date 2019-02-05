""" Driver for AMQP support """

import pickle
import threading

from config import queue
from masonite.contracts import QueueContract
from masonite.drivers import BaseDriver
from masonite.app import App
from masonite.exceptions import DriverLibraryNotFound

if 'amqp' in queue.DRIVERS:
    listening_channel = queue.DRIVERS['amqp']['channel']
else:
    listening_channel = 'default'


class QueueAmqpDriver(QueueContract, BaseDriver):

    def __init__(self):
        """Queue AMQP Driver

        Arguments:
            Container {masonite.app.App} -- The application container.
        """

        # Start the connection
        self.connect()

    def _publish(self, body):
        self.channel.basic_publish(exchange='',
                                   routing_key=listening_channel,
                                   body=pickle.dumps(
                                       body
                                   ),
                                   properties=self.pika.BasicProperties(
                                       delivery_mode=2,  # make message persistent
                                   ))

    def push(self, *objects, args=(), callback='handle'):
        """Push objects onto the amqp stack.

        Arguments:
            objects {*args of objects} - This can be several objects as parameters into this method.
        """

        for obj in objects:
            # Publish to the channel for each object
            try:
                self._publish({'obj': obj, 'args': args, 'callback': callback})
            except self.pika.exceptions.ConnectionClosed:
                self.connect()
                self._publish({'obj': obj, 'args': args})

    def connect(self):
        try:
            import pika
            self.pika = pika
        except ImportError:
            raise DriverLibraryNotFound(
                "Could not find the 'pika' library. Run pip install pika to fix this.")

        self.connection = pika.BlockingConnection(pika.URLParameters('amqp://{}:{}@{}{}/{}'.format(
            queue.DRIVERS['amqp']['username'],
            queue.DRIVERS['amqp']['password'],
            queue.DRIVERS['amqp']['host'],
            ':' + str(queue.DRIVERS['amqp']['port']) if 'port' in queue.DRIVERS['amqp'] and queue.DRIVERS['amqp']['port'] else '',
            queue.DRIVERS['amqp']['vhost'] if 'vhost' in queue.DRIVERS['amqp'] and queue.DRIVERS['amqp']['vhost'] else '%2F'
        )))

        return self

    def consume(self, queue_channel, durable=True, fair=False):
        self.channel = self.connection.channel()

        self.channel.queue_declare(queue=queue_channel, durable=durable)

        self.channel.basic_consume(self.work,
                              queue=queue_channel)

        if fair:
            self.channel.basic_qos(prefetch_count=1)

        print('[*] Waiting to process jobs on the "{}" channel. To exit press CTRL+C'.format(
            queue_channel))

        return self.channel.start_consuming()

    def work(self, ch, method, properties, body):
        from wsgi import container
        job = pickle.loads(body)
        obj = job['obj']
        args = job['args']
        callback = job['callback']

        try:
            try:
                if inspect.isclass(obj):
                    obj = container.resolve(obj)
                getattr(obj, callback)(*args)
            except AttributeError:
                obj(*args)

            self.info('[\u2713] Job Successfully Processed')
        except Exception as e:
            print(pickle.loads(body))
            self.warning('Job Failed: {}'.format(str(e)))

        ch.basic_ack(delivery_tag=method.delivery_tag)
