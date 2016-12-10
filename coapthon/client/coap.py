from concurrent.futures import ThreadPoolExecutor
import logging
import logging.config
import random
import socket
import threading
from coapthon.messages.message import Message
from coapthon.messages.response import Response
from coapthon import defines
from coapthon.layers.blocklayer import BlockLayer
from coapthon.layers.messagelayer import MessageLayer
from coapthon.layers.observelayer import ObserveLayer
from coapthon.layers.requestlayer import RequestLayer
from coapthon.messages.request import Request
from coapthon.serializer import Serializer
import os.path

__author__ = 'giacomo'

logger = logging.getLogger(__name__)

class CoAP(object):
    def __init__(self, server, starting_mid, receive_callback, timeout_callback=None):
        logger.debug("Starting CoAP client")
        self._currentMID = starting_mid
        self._server = server
        self._receive_callback = receive_callback
        self._timeout_callback = timeout_callback or (lambda x: x)
        self.stopped = threading.Event()
        self.to_be_stopped = []

        self._messageLayer = MessageLayer(self._currentMID)
        self._blockLayer = BlockLayer()
        self._observeLayer = ObserveLayer()
        self._requestLayer = RequestLayer(self)

        host, port = self._server
        addrinfo = socket.getaddrinfo(host, None)[0]

        if addrinfo[0] == socket.AF_INET:
            self._socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        else:
            self._socket = socket.socket(socket.AF_INET6, socket.SOCK_DGRAM)
            self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        logger.debug("Creating threadpool with 2 workers")
        self.thread_pool = ThreadPoolExecutor(max_workers=2)
        logger.debug("Submitting receive_datagram to thread pool")
        self.thread_pool.submit(self.receive_datagram)
        logger.debug("Finished submitting receive_datagram to thread pool")

    @property
    def current_mid(self):
        return self._currentMID

    @current_mid.setter
    def current_mid(self, c):
        assert isinstance(c, int)
        self._currentMID = c

    def send_message(self, message):

        if isinstance(message, Request):
            request = self._requestLayer.send_request(message)
            request = self._observeLayer.send_request(request)
            request = self._blockLayer.send_request(request)
            transaction = self._messageLayer.send_request(request)
            if transaction.request.type == defines.Types["CON"]:
                self._start_retransmission(transaction, transaction.request)

            self.send_datagram(transaction.request)
        elif isinstance(message, Message):
            message = self._observeLayer.send_empty(message)
            message = self._messageLayer.send_empty(None, None, message)
            self.send_datagram(message)

    def send_datagram(self, message):
        host, port = message.destination
        logger.debug("send_datagram - " + str(message))
        serializer = Serializer()
        message = serializer.serialize(message)

        logger.debug("Sending datagram")
        self._socket.sendto(message, (host, port))

    def _start_retransmission(self, transaction, message):
        """
        Start the retransmission task.

        :type transaction: Transaction
        :param transaction: the transaction that owns the message that needs retransmission
        :type message: Message
        :param message: the message that needs the retransmission task
        """
        with transaction:
            if message.type == defines.Types['CON']:
                future_time = random.uniform(defines.ACK_TIMEOUT, (defines.ACK_TIMEOUT * defines.ACK_RANDOM_FACTOR))

                transaction.retransmit_stop = threading.Event()
                self.to_be_stopped.append(transaction.retransmit_stop)

                logger.debug("Submitting retransmit to thread pool")
                self.thread_pool.submit(self._retransmit, (transaction, message, future_time, 0))
                logger.debug("Finished submitting retransmit to thread pool")

    def _retransmit(self, transaction, message, future_time, retransmit_count):
        """
        Thread function to retransmit the message in the future

        :param transaction: the transaction that owns the message that needs retransmission
        :param message: the message that needs the retransmission task
        :param future_time: the amount of time to wait before a new attempt
        :param retransmit_count: the number of retransmissions
        """
        logger.debug("Inside _retransmit")
        with transaction:
            logger.debug("About to retransmit packet")
            logger.debug("retransmit_count: %s, acknowledged: %s, rejected: %s, isSet: %s",
                         retransmit_count,
                         message.acknowledged,
                         message.rejected,
                         self.stopped.isSet())
            while retransmit_count < defines.MAX_RETRANSMIT and (not message.acknowledged and not message.rejected) \
                    and not self.stopped.isSet():
                logger.debug("Waiting for %s before retransmitting", future_time)
                transaction.retransmit_stop.wait(timeout=future_time)
                if not message.acknowledged and not message.rejected and not self.stopped.isSet():
                    logger.debug("retransmit Request")
                    retransmit_count += 1
                    future_time *= 2
                    self.send_datagram(message)

            if message.acknowledged or message.rejected:
                message.timeouted = False
            else:
                logger.warning("Give up on message {message}".format(message=message.line_print))
                message.timeouted = True
                self._timeout_callback(message)
                logger.debug("Finished with _timeout_callback")

            try:
                logger.debug("Removing retransmit_stop from to_be_stopped")
                self.to_be_stopped.remove(transaction.retransmit_stop)
            except ValueError:
                pass
            transaction.retransmit_stop = None
            transaction.retransmit_thread = None
            logger.debug("Finished with _retransmit")

    def receive_datagram(self):
        logger.debug("Start receiver Thread")
        while not self.stopped.isSet():
            self._socket.settimeout(1)
            try:
                datagram, addr = self._socket.recvfrom(1152)
            except socket.timeout:  # pragma: no cover
                continue
            except socket.error:  # pragma: no cover
                return
            else:  # pragma: no cover
                if len(datagram) == 0:
                    print('orderly shutdown on server end')
                    return

            serializer = Serializer()
            logger.debug("receive_datagram: Received a packet")

            try:
                host, port = addr
            except ValueError:
                host, port, tmp1, tmp2 = addr

            source = (host, port)

            message = serializer.deserialize(datagram, source)

            if isinstance(message, Response):
                transaction, send_ack = self._messageLayer.receive_response(message)
                if transaction is None:  # pragma: no cover
                    continue
                if send_ack:
                    self._send_ack(transaction)
                self._blockLayer.receive_response(transaction)
                if transaction.block_transfer:
                    transaction = self._messageLayer.send_request(transaction.request)
                    self.send_datagram(transaction.request)
                    continue
                elif transaction is None:  # pragma: no cover
                    self._send_rst(transaction)
                    return
                self._observeLayer.receive_response(transaction)
                if transaction.notification:  # pragma: no cover
                    ack = Message()
                    ack.type = defines.Types['ACK']
                    ack = self._messageLayer.send_empty(transaction, transaction.response, ack)
                    self.send_datagram(ack)
                    self._receive_callback(transaction.response)
                else:
                    self._receive_callback(transaction.response)
            elif isinstance(message, Message):
                self._messageLayer.receive_empty(message)

    def _send_ack(self, transaction):
        # Handle separate
        """
        Sends an ACK message for the response.

        :param transaction: transaction that holds the response
        """

        ack = Message()
        ack.type = defines.Types['ACK']

        if not transaction.response.acknowledged:
            ack = self._messageLayer.send_empty(transaction, transaction.response, ack)
            self.send_datagram(ack)

    def _send_rst(self, transaction):  # pragma: no cover
        # Handle separate
        """
        Sends an RST message for the response.

        :param transaction: transaction that holds the response
        """

        rst = Message()
        rst.type = defines.Types['RST']

        if not transaction.response.acknowledged:
            rst = self._messageLayer.send_empty(transaction, transaction.response, rst)
            self.send_datagram(rst)
