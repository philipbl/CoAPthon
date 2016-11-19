import struct
import ctypes
from coapthon.messages.request import Request
from coapthon.messages.response import Response
from coapthon.messages.option import Option
from coapthon import defines
from coapthon.messages.message import Message


class Serializer(object):

    @staticmethod
    def deserialize(datagram, source):
        """
        De-serialize a stream of byte to a message.

        :type datagram: String
        :param datagram:
        :param source:
        """
        try:
            (vttkl, code, mid) = struct.unpack('!BBH', datagram[:4])
        except struct.error:
            raise error.UnparsableMessage("Incoming message too short for CoAP")
        version = (vttkl & 0xC0) >> 6
        if version is not 1:
            raise error.UnparsableMessage("Fatal Error: Protocol Version must be 1")
        mtype = (vttkl & 0x30) >> 4
        token_length = (vttkl & 0x0F)

        if Serializer.is_response(code):
            message = Response()
            message.code = code
        elif Serializer.is_request(code):
            message = Request()
            message.code = code
        else:
            message = Message()

        def _read_extended_field_value(value, rawdata):
            """Used to decode large values of option delta and option length
               from raw binary form."""
            if value >= 0 and value < 13:
                return (value, rawdata)
            elif value == 13:
                return (rawdata[0] + 13, rawdata[1:])
            elif value == 14:
                return (struct.unpack('!H', rawdata[:2])[0] + 269, rawdata[2:])
            else:
                raise ValueError("Value out of range.")

        def decode_options(rawdata):
            option_number = 0

            while len(rawdata) > 0:
                if rawdata[0] == 0xFF:
                    return rawdata[1:]

                dllen = rawdata[0]
                delta = (dllen & 0xF0) >> 4
                length = (dllen & 0x0F)
                rawdata = rawdata[1:]
                (delta, rawdata) = _read_extended_field_value(delta, rawdata)
                (length, rawdata) = _read_extended_field_value(length, rawdata)

                option_number += delta
                option_item = defines.OptionRegistry.LIST[option_number]

                if length == 0:
                    value = None
                elif option_item.value_type == defines.INTEGER:
                    value = 0
                    for byte in rawdata[:length]:
                        value = (value * 256) + byte
                else:
                    value = rawdata[:length].decode('utf-8')

                option = Option()
                option.number = option_number
                option.value = Serializer.convert_to_raw(option_number, value, length)
                message.add_option(option)
                rawdata = rawdata[length:]
            return b''

        message.source = source
        message.destination = None
        message.version = version
        message.type = mtype
        message.mid = mid
        message.token = None if token_length == 0 else datagram[4:4 + token_length]
        message.payload = decode_options(datagram[4 + token_length:])

        return message

    @staticmethod
    def serialize(message):
        """

        :type message: Message
        :param message:
        """

        def _write_extended_field_value(value):
            """Used to encode large values of option delta and option length
               into raw binary form.
               In CoAP option delta and length can be represented by a variable
               number of bytes depending on the value."""
            if value >= 0 and value < 13:
                return (value, b'')
            elif value >= 13 and value < 269:
                return (13, struct.pack('!B', value - 13))
            elif value >= 269 and value < 65804:
                return (14, struct.pack('!H', value - 269))
            else:
                raise ValueError("Value out of range.")

        def encode_options():
            data = []
            current_opt_num = 0
            options = Serializer.as_sorted_list(message.options)
            for option in options:
                delta, extended_delta = _write_extended_field_value(option.number - current_opt_num)
                length, extended_length = _write_extended_field_value(option.length)
                data.append(bytes([((delta & 0x0F) << 4) + (length & 0x0F)]))
                data.append(extended_delta)
                data.append(extended_length)

                option_item = defines.OptionRegistry.LIST[option.number]
                if option_item.value_type == defines.INTEGER:
                    rawdata = struct.pack("!L", option.value)
                    data.append(rawdata.lstrip(bytes([0])))
                elif isinstance(option.value, bytes):
                    data.append(option.value)
                else:
                    data.append(option.value.encode('utf-8'))

                current_opt_num = option.number
            return (b''.join(data))

        rawdata = bytes([(message.version << 6) +
                        ((message.type & 0x03) << 4) +
                        (len(message.token or []) & 0x0F)])
        rawdata += struct.pack('!BH', message.code, message.mid)
        rawdata += message.token if message.token is not None else b''
        rawdata += encode_options()


        if message.payload is not None and len(message.payload) > 0:
            rawdata += bytes([0xFF])
            rawdata += message.payload

        return rawdata

    @staticmethod
    def is_request(code):
        """
        Checks if is request.

        :return: True, if is request
        """
        return defines.REQUEST_CODE_LOWER_BOUND <= code <= defines.REQUEST_CODE_UPPER_BOUND

    @staticmethod
    def is_response(code):
        """
        Checks if is response.

        :return: True, if is response
        """
        return defines.RESPONSE_CODE_LOWER_BOUND <= code <= defines.RESPONSE_CODE_UPPER_BOUND

    @staticmethod
    def read_option_value_from_nibble(nibble, pos, values):
        """
        Calculates the value used in the extended option fields.

        :param nibble: the 4-bit option header value.
        :return: the value calculated from the nibble and the extended option value.
        """
        if nibble <= 12:
            return nibble, pos
        elif nibble == 13:
            tmp = values[pos] + 13
            pos += 1
            return tmp, pos
        elif nibble == 14:
            tmp = values[pos] + 269
            pos += 2
            return tmp, pos
        else:
            raise AttributeError("Unsupported option nibble " + str(nibble))

    @staticmethod
    def convert_to_raw(number, value, length):
        """
        Get the value of an option as a ByteArray.

        :param number: the option number
        :param value: the option value
        :param length: the option length
        :return: the value of an option as a BitArray
        """

        opt_type = defines.OptionRegistry.LIST[number].value_type

        if length == 0 and opt_type != defines.INTEGER:
            return bytearray()
        if length == 0 and opt_type == defines.INTEGER:
            return 0

        if isinstance(value, tuple):
            value = value[0]

        if isinstance(value, str):
            return value
        elif isinstance(value, int):
            return value
        else:
            return value

    @staticmethod
    def as_sorted_list(options):
        """
        Returns all options in a list sorted according to their option numbers.

        :return: the sorted list
        """
        if len(options) > 0:
            options.sort(key=lambda o: o.number)
        return options

    @staticmethod
    def get_option_nibble(optionvalue):
        """
        Returns the 4-bit option header value.

        :param optionvalue: the option value (delta or length) to be encoded.
        :return: the 4-bit option header value.
         """
        if optionvalue <= 12:
            return optionvalue
        elif optionvalue <= 255 + 13:
            return 13
        elif optionvalue <= 65535 + 269:
            return 14
        else:
            raise AttributeError("Unsupported option delta " + optionvalue)

    @staticmethod
    def int_to_words(int_val, num_words=4, word_size=32):
        """
        @param int_val: an arbitrary length Python integer to be split up.
            Network byte order is assumed. Raises an IndexError if width of
            integer (in bits) exceeds word_size * num_words.

        @param num_words: number of words expected in return value tuple.

        @param word_size: size/width of individual words (in bits).

        @return: a list of fixed width words based on provided parameters.
        """
        max_int = 2 ** (word_size*num_words) - 1
        max_word_size = 2 ** word_size - 1

        if not 0 <= int_val <= max_int:
            raise AttributeError('integer %r is out of bounds!' % hex(int_val))

        words = []
        for _ in range(num_words):
            word = int_val & max_word_size
            words.append(int(word))
            int_val >>= word_size
        words.reverse()

        return words
