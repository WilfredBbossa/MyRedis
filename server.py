from gevent import socket
from gevent.pool import Pool
from gevent.server import StreamServer

from collections import namedtuple
from io import BytesIO
from socket import error as SocketError

# Using exception for notification of command errors and disconnection


class CommandError(Exception):
    def __init__(self, code, message):
        self.code = code
        self.message = message
        super().__init__(message)


class Disconnect(Exception):
    pass


Error = namedtuple('Error', ['code', 'message'])


class ProtocolHandler(object):
    def __init__(self):
        self.handlers = {
            b'+': self.handle_simple_string,
            b'-': self.handle_error,
            b':': self.handle_integer,
            b'$': self.handle_bulk_string,
            b'*': self.handle_array,
            b'%': self.handle_dictionary
        }

    def handle_request(self, socket_file):
        # Parse a request from the client into its component parts
        first_byte = socket_file.read(1)
        if not first_byte:
            raise Disconnect('Client disconnected')

        try:
            # Delegate the request handling to the appropriate handler based on the first byte of the request.
            return self.handlers[first_byte](socket_file)
        except KeyError:
            raise CommandError(
                'ERR', 'Unknown request type: {}'.format(first_byte))

    def handle_simple_string(self, socket_file):
        # Handle a simple string request from the client
        line = socket_file.readline().rstrip(b'\r\n')
        return line.decode('utf-8')

    def handle_error(self, socket_file):
        # Handle an error request from the client
        line = socket_file.readline().rstrip(b'\r\n')
        return Error('ERR', line.decode('utf-8'))

    def handle_integer(self, socket_file):
        # Handle an integer request from the client
        line = socket_file.readline().rstrip(b'\r\n')
        return int(line)

    def handle_bulk_string(self, socket_file):
        # Handle a bulk string request from the client
        length_line = socket_file.readline().rstrip(b'\r\n')
        length = int(length_line)
        if length == -1:
            return None  # Null bulk string
        data = socket_file.read(length)
        socket_file.read(2)  # Read the trailing CRLF
        return data.decode('utf-8')

    def handle_array(self, socket_file):
        # Handle an array request from the client
        length_line = socket_file.readline().rstrip(b'\r\n')
        length = int(length_line)
        if length == -1:
            return None  # Null array
        array = []
        for _ in range(length):
            array.append(self.handle_request(socket_file))
        return array

    def handle_dictionary(self, socket_file):
        # Handle a dictionary request from the client
        length_line = socket_file.readline().rstrip(b'\r\n')
        length = int(length_line)
        if length == -1:
            return None  # Null dictionary
        dictionary = {}
        for _ in range(length):
            key = self.handle_request(socket_file)
            value = self.handle_request(socket_file)
            dictionary[key] = value
        return dictionary

    def write_response(self, socket_file, data):
        # Write a response to the client
        buffer = BytesIO()
        self.write(buffer, data)
        buffer.seek(0)
        socket_file.write(buffer.read())
        socket_file.flush()

    def write(self, buffer, data):
        # Write data to the buffer in the appropriate format based on its type
        if isinstance(data, str):
            buffer.write(b'+' + data.encode('utf-8') + b'\r\n')
        elif isinstance(data, int):
            buffer.write(b':' + str(data).encode('utf-8') + b'\r\n')
        elif isinstance(data, Error):
            buffer.write(b'-' + data.message.encode('utf-8') + b'\r\n')
        elif isinstance(data, (list, tuple)):
            buffer.write(b'*' + str(len(data)).encode('utf-8') + b'\r\n')
            for item in data:
                self.write(buffer, item)
        elif isinstance(data, dict):
            buffer.write(b'%' + str(len(data)).encode('utf-8') + b'\r\n')
            for key, value in data.items():
                self.write(buffer, key)
                self.write(buffer, value)
        elif data is None:
            buffer.write(b'$-1\r\n')
        else:
            raise CommandError(
                'ERR', 'Unsupported response type: {}'.format(type(data)))


class RedisServer(StreamServer):
    def __init__(self, listener, max_clients=64, **kwargs):

        self._commands = self.get_commands()
        self.pool = Pool(max_clients)

        super(RedisServer, self).__init__(
            listener, spawn=self.pool, **kwargs
        )

        self._protocol = ProtocolHandler()
        self._kv = {}

    def handle(self, conn, address):
        # StreamServer calls this for each incoming connection;
        # delegate to our connection handler.
        self.connection_handler(conn, address)

    def connection_handler(self, conn, address):
        # COnvert "conn" (a socket object) into a file-like object
        socket_file = conn.makefile('rwb')
        # 'rwb' allows us to read and write binary data through the socket.

        # process client requests until client disconnects or an error occurs
        try:
            while True:
                try:
                    # Read and decode the incoming client request.
                    request = self._protocol.handle_request(socket_file)
                except Disconnect:
                    # Stop processing requests if the client disconnects.

                    break

                try:
                    # Process the client's request and generate an appropriate response.
                    response = self.get_response(request)
                except CommandError as exc:
                    # If the client sends an invalid command or a command
                    # cannot be executed, create an error response.
                    # The connection remains open so the client can send
                    # another request.
                    response = Error(exc.code, exc.message)

                # Send the response (either a successful result or an error)
                # back to the client through the socket.
                self._protocol.write_response(socket_file, response)

        except SocketError:
            # Terminate the connection if a socket-related error occurs.
            pass
        finally:
            # Close the file-like object to release the associated resources.
            socket_file.close()

    def get_commands(self):
        # Map supported Redis commands to their corresponding methods.
        return {
            'GET': self.get,
            'SET': self.set,
            'DEL': self.delete,
            'DELETE': self.delete,
            'FLUSH': self.flush,
            'MGET': self.mget,
            'MSET': self.mset
        }

    def get_response(self, request):
        # Process the client's request and generate an appropriate response.

        # Check whether the request is a list.
        # If it is not, attempt to convert it into a list.
        if not isinstance(request, list):
            if isinstance(request, str):
                request = request.split()
            else:
                raise CommandError('ERR', 'Invalid request format')

        # Check whether the request contains a command.
        if not request:
            raise CommandError('ERR', 'Missing command')

        # Extract the command name and convert it to uppercase
        # so that commands are case-insensitive.
        command = request[0].upper()

        # Check whether the requested command is supported.
        if command not in self._commands:
            raise CommandError('ERR', 'Unrecognized command: %s' % command)

        # Execute the corresponding command method, passing
        # the remaining request elements as arguments.
        return self._commands[command](*request[1:])

    def get(self, key):
        # Retrieve the value associated with the specified key from the key-value store.
        return self._kv.get(key, None)

    def set(self, key, value):
        # Store the specified key-value pair in the key-value store.
        self._kv[key] = value
        return 'OK'

    def delete(self, key):
        # Remove the specified key from the key-value store.
        if key in self._kv:
            del self._kv[key]
            return 1  # Return 1 to indicate successful deletion
        return 0  # Return 0 to indicate that the key was not found

    def flush(self):
        # Clear all key-value pairs from the key-value store.
        kvlen = len(self._kv)
        self._kv.clear()
        return 'OK ({} keys deleted)'.format(kvlen)

    def mget(self, *keys):
        # Retrieve the values associated with the specified keys from the key-value store.
        return [self._kv.get(key, None) for key in keys]

    def mset(self, *args):
        # Store multiple key-value pairs in the key-value store.
        # Ensure that every key has a corresponding value.
        if len(args) % 2 != 0:
            raise CommandError(
                'ERR', 'MSET requires an even number of arguments')

        # Group alternating arguments into key-value pairs.
        data = zip(args[::2], args[1::2])

        # Store each key-value pair in the dictionary.
        for key, value in data:
            self._kv[key] = value

        # Return OK to indicate successful execution.
        return 'OK'

    def run(self):
        # Start the server and listen for incoming connections.
        super(RedisServer, self).serve_forever()


class Client(object):
    def __init__(self, host='127.0.0.1', port=31337):
        self._protocol = ProtocolHandler()
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.connect((host, port))
        self._fh = self._socket.makefile('rwb')

    def execute(self, *args):
        self._protocol.write_response(self._fh, args)
        resp = self._protocol.handle_request(self._fh)
        if isinstance(resp, Error):
            raise CommandError(resp.code, resp.message)
        return resp

    def get(self, key):
        return self.execute('GET', key)

    def set(self, key, value):
        return self.execute('SET', key, value)

    def delete(self, key):
        return self.execute('DELETE', key)

    def flush(self):
        return self.execute('FLUSH')

    def mget(self, *keys):
        return self.execute('MGET', *keys)

    def mset(self, *items):
        return self.execute('MSET', *items)


if __name__ == '__main__':
    from gevent import monkey
    monkey.patch_all()
    RedisServer(('127.0.0.1', 31337)).serve_forever()
