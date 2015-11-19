import SocketServer
import logging
import logging.config
import multiprocessing
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
        SocketServer.BaseRequestHandler.__init__(self, transaction, client_address, server)
        self._client_socket = None
        self._server = server

    def handle(self):
        """
        Handler for received UDP datagram.

        """

        transaction = self.request[0]
        self._client_socket = self.request[1]





    def _handle_request(self, message):
        with self._server.lock:
            transaction = self._server.messageLayer.receive_request(message)
            if transaction.request.duplicated and transaction.completed:
                logger.debug("message duplicated,transaction completed")
                transaction = self._server.observeLayer.send_response(transaction)
                transaction = self._server.blockLayer.send_response(transaction)
                transaction.response.type = None
                transaction.request.acknowledged = False
                transaction = self._messageLayer.send_response(transaction)
                self.send_datagram(transaction.response)
                return
            elif transaction.request.duplicated and not transaction.completed:
                logger.debug("message duplicated,transaction NOT completed")
                self._send_ack(transaction)
                return


class CoAPServer(SocketServer.ThreadingUDPServer):
    def __init__(self, server_address, multicast=False, starting_mid=None):
        """
        Initialize the CoAP protocol
        """
        SocketServer.UDPServer.__init__(self, server_address, CoAPHandler)
        self.lock = multiprocessing.RLock()
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

    def finish_request(self, request, client_address):
        serializer = Serializer()
        message = serializer.deserialize(request, client_address)
        if isinstance(message, Request):
            transaction = self.messageLayer.receive_request(request)
            if transaction.request.duplicated and transaction.completed:
                logger.debug("message duplicated,transaction completed")
                self.send_datagram(transaction.response)
                return
            elif transaction.request.duplicated and not transaction.completed:
                logger.debug("message duplicated,transaction NOT completed")
                self._send_ack(transaction)
                return
            self.RequestHandlerClass(transaction, client_address, self)
        elif isinstance(message, Message):
            transaction = self.messageLayer.receive_empty(message)
            if transaction is not None:
                transaction = self.blockLayer.receive_empty(message, transaction)
                transaction = self.observeLayer.receive_empty(message, transaction)
        else:  # is Response
            logger.error("Received response from %s", message.source)

    def _send_ack(self, transaction):
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