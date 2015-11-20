import SocketServer
import logging
import logging.config
import random
import threading
from coapthon import defines
from coapthon.layers.blocklayer import BlockLayer
from coapthon.layers.messagelayer import MessageLayer
from coapthon.layers.observelayer import ObserveLayer
from coapthon.layers.requestlayer import RequestLayer
from coapthon.layers.resourcelayer import ResourceLayer
from coapthon.messages.message import Message
from coapthon.messages.request import Request
from coapthon.resources.resource import Resource
from coapthon.serializer import Serializer
from coapthon.utils import Tree

__author__ = 'jacko'

logger = logging.getLogger(__name__)
logging.config.fileConfig("logging.conf", disable_existing_loggers=False)


class CoAPHandler(SocketServer.BaseRequestHandler):
    def __init__(self, transaction, client_address, server):
        self._client_socket = None
        self._server = server
        SocketServer.BaseRequestHandler.__init__(self, transaction, client_address, server)

    def _start_separate_timer(self, transaction):
        """

        :type transaction: Transaction
        :param transaction:
        :type message: Message
        :param message:
        :rtype : Future
        """
        t = threading.Timer(defines.ACK_TIMEOUT, self._server.send_ack, (transaction,))
        t.start()
        return t

    @staticmethod
    def _stop_separate_timer(timer):
        """

        :type future: Future
        :param future:
        """
        timer.cancel()

    def handle(self):
        """
        Handler for received UDP datagram.

        """

        transaction = self.request[0]
        self._client_socket = self.request[1]
        with transaction:
            transaction.separate_timer = self._start_separate_timer(transaction)

            self._server.blockLayer.receive_request(transaction)

            if transaction.block_transfer:
                self._stop_separate_timer(transaction.separate_timer)
                transaction = self._server.messageLayer.send_response(transaction)
                self._server.send_datagram(transaction.response)
                return

            self._server.observeLayer.receive_request(transaction)

            self._server.requestLayer.receive_request(transaction)

            if transaction.resource is not None and transaction.resource.changed:
                self._server.notify(transaction.resource)
                transaction.resource.changed = False
            elif transaction.resource is not None and transaction.resource.deleted:
                self._server.notify(transaction.resource)
                transaction.resource.deleted = False

            self._server.observeLayer.send_response(transaction)

            self._server.blockLayer.send_response(transaction)

            self._stop_separate_timer(transaction.separate_timer)

            self._server.messageLayer.send_response(transaction)

            if transaction.response is not None:
                if transaction.response.type == defines.Types["CON"]:
                    self._server.start_retrasmission(transaction, transaction.response)
                self._server.send_datagram(transaction.response)


class CoAP(SocketServer.ThreadingUDPServer):
    def __init__(self, server_address, multicast=False, starting_mid=None):
        """
        Initialize the CoAP protocol
        """
        SocketServer.UDPServer.__init__(self, server_address, CoAPHandler)
        self.stopped = threading.Event()
        self.stopped.clear()
        self.to_be_stopped = []
        self.messageLayer = MessageLayer(starting_mid)
        self.blockLayer = BlockLayer()
        self.observeLayer = ObserveLayer()
        self.requestLayer = RequestLayer(self)
        self.resourceLayer = ResourceLayer(self)

        # Resource directory
        root = Resource('root', self, visible=False, observable=False, allow_children=True)
        root.path = '/'
        self.root = Tree()
        self.root["/"] = root

    def process_request(self, request, client_address):
        serializer = Serializer()
        message = serializer.deserialize(request[0], client_address)
        if isinstance(message, Request):
            transaction = self.messageLayer.receive_request(message)
            with transaction:
                if transaction.request.duplicated and transaction.completed:
                    logger.debug("message duplicated,transaction completed")
                    self.send_datagram(transaction.response)
                    return
                elif transaction.request.duplicated and not transaction.completed:
                    logger.debug("message duplicated,transaction NOT completed")
                    self.send_ack(transaction)
                    return
                """Start a new thread to process the request."""
                t = threading.Thread(target = self.process_request_thread,
                                     args = ((transaction, request[1]) , client_address))
                t.daemon = self.daemon_threads
                t.start()

        elif isinstance(message, Message):
            transaction = self.messageLayer.receive_empty(message)
            with transaction:
                if transaction is not None:
                    transaction = self.blockLayer.receive_empty(message, transaction)
                    self.observeLayer.receive_empty(message, transaction)
        else:  # is Response
            logger.error("Received response from %s", message.source)

    def add_resource(self, path, resource):
        """
        Helper function to add resources to the resource directory during server initialization.

        :type resource: Resource
        :param resource:
        """

        assert isinstance(resource, Resource)
        path = path.strip("/")
        paths = path.split("/")
        actual_path = ""
        i = 0
        for p in paths:
            i += 1
            actual_path += "/" + p
            try:
                res = self.root[actual_path]
            except KeyError:
                res = None
            if res is None:
                if len(paths) != i:
                    return False
                resource.path = actual_path
                self.root[actual_path] = resource
        return True

    def start_retrasmission(self, transaction, message):
        """

        :type transaction: Transaction
        :param transaction:
        :type message: Message
        :param message:
        :rtype : Future
        """
        with transaction:
            if message.type == defines.Types['CON']:
                future_time = random.uniform(defines.ACK_TIMEOUT, (defines.ACK_TIMEOUT * defines.ACK_RANDOM_FACTOR))
                transaction.retransmit_thread = threading.Thread(target=self._retransmit,
                                                                 args=(transaction, message, future_time, 0))
                transaction.retransmit_stop = threading.Event()
                self.to_be_stopped.append(transaction.retransmit_stop)
                transaction.retransmit_thread.start()

    def _retransmit(self, transaction, message, future_time, retransmit_count):
        with transaction:
            while retransmit_count < defines.MAX_RETRANSMIT and (not message.acknowledged and not message.rejected) \
                    and not self.stopped.isSet():
                transaction.retransmit_stop.wait(timeout=future_time)
                if not message.acknowledged and not message.rejected and not self.stopped.isSet():
                    retransmit_count += 1
                    future_time *= 2
                    self.send_datagram(message)

            if message.acknowledged or message.rejected:
                message.timeouted = False
            else:
                logger.warning("Give up on message {message}".format(message=message.line_print))
                message.timeouted = True
                if message.observe is not None:
                    self.observeLayer.remove_subscriber(message)

            try:
                self.to_be_stopped.remove(transaction.retransmit_stop)
            except ValueError:
                pass
            transaction.retransmit_stop = None
            transaction.retransmit_thread = None

    def send_ack(self, transaction):
        # Handle separate
        """
        Sends an ACK message for the request.

        :param request: [request, sleep_time] or request
        """
        ack = Message()
        ack.type = defines.Types['ACK']

        if not transaction.request.acknowledged and transaction.request.type == defines.Types["CON"]:
            ack = self.messageLayer.send_empty(transaction, transaction.request, ack)
            self.send_datagram(ack)

    def send_datagram(self, message):
        """

        :type message: Message
        :param message:
        """

        host, port = message.destination
        logger.debug("send_datagram - " + str(message))
        serializer = Serializer()
        message = serializer.serialize(message)

        self.socket.sendto(message, (host, port))

    def notify(self, resource):
        observers = self.observeLayer.notify(resource)
        logger.debug("Notify")
        for transaction in observers:
            with transaction:
                transaction.response = None
                transaction = self.requestLayer.receive_request(transaction)
                transaction = self.observeLayer.send_response(transaction)
                transaction = self.blockLayer.send_response(transaction)
                transaction = self.messageLayer.send_response(transaction)
                if transaction.response is not None:
                    if transaction.response.type == defines.Types["CON"]:
                        self.start_retrasmission(transaction, transaction.response)

                    self.send_datagram(transaction.response)