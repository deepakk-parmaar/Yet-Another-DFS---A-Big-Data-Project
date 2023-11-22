#!/usr/bin/env python3.11
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.client import ServerProxy
import sys
import os
import errno
import time
import socket
import _thread
import threading
import ssl
import logging
# from os.path import dirname
# sys.path.append(dirname(dirname(__file__)))
# from utils.enums import Status
#!/usr/bin/env python3.11
class NodeType:
    directory = 2
    file = 1
    @staticmethod
    def description(stat):
        node = ''
        if stat == 2:
            node = 'directory'
        elif stat == 1:
            node = 'file'
        return node

class Status:
    ok = 200
    error = 500
    already_exists = 409
    not_found = 404
    
    @staticmethod
    def description(stat):
        message = ''
        if stat == 409:
            message = 'Error 409 - Item Already Exists!'
        elif stat == 404:
            message = 'Error 404 - Item Not Found!'
        elif stat == 200:
            message = 'Status 200 - Okay.'
        elif stat == Status.error:
            message = 'Error 500 - Internal error.'
        return message


class ChunkServer:
    def __init__(self, addr, ns_addr):
        self.ns = ServerProxy(ns_addr)
        self.addr = addr
        self.ns_addr = ns_addr
        self.local_fs_root = "/tmp/yadfs/chunks"
        self.hb_timeout = 0.5  # heartbeat timeout in seconds
        self.on = True

    def start(self):
        print('Init server')
        if not os.access(self.local_fs_root, os.W_OK):
            print('Create directory for storage:', self.local_fs_root,)
            os.makedirs(self.local_fs_root)

        print('Start sending heartbeats to', self.ns_addr)
        _thread.start_new_thread(self._heartbeat, ())
        print('Server is ready')

    def _heartbeat(self):
        while self.on:
            try:
                self.ns.heartbeat(self.addr)
            except Exception as e:
                pass
            time.sleep(self.hb_timeout)
    
    def implement_chunk_caching_mechanisms():
        pass
    # Cache frequently accessed chunks in memory or a faster storage medium
    # to improve access performance
    # ...

    # Simulate managing the cached chunks and evicting them when necessary
    # ... Later

    
    def upload_chunk(self, chunk_path, chunk):
        print('Upload file', chunk_path)
        local_path = self.chunk_filename(chunk_path)
        ldir = os.path.dirname(local_path)
        self.make_sure_path_exists(ldir)
        with open(local_path, "w") as f:
            f.write(chunk)
            return {'status': Status.ok}

    #check
    def get_file_size(self, filename):
        try:
            stat_info = os.stat(filename)
            return stat_info.st_size
        except Exception as e:
            print("Error getting file size:", e)
            return -1 

    def get_chunk(self, chunk_path):
        local_path = self.chunk_filename(chunk_path)
        with open(local_path, "r") as f:
            data = f.read()
        return data

    #check
    def check_file_permissions(self, filename):
        try:
            os.access(filename, os.R_OK)
            return True
        except Exception as e:
            print("Error checking file permissions:", e)
            return False

    def delete_chunk(self, chunk_path):
        local_path = self.chunk_filename(chunk_path)
        ldir = os.path.dirname(local_path)
        self.make_sure_path_exists(ldir)
        if os.path.exists(local_path):
            os.remove(local_path)
            print('Delete file', chunk_path)
            return {'status': Status.ok}
        else:
            return {'status': Status.not_found}

    #check
    def download_file(self, url, filename):
        try:
            with open(filename, 'wb') as f:
                response = requests.get(self.chunk_filename(chunk_path), stream=True)
                for chunk in response.iter_content(chunk_size=1024):
                    f.write(chunk)
            return True
        except Exception as e:
            print("Error downloading file:", e)
            return False
    
    def replicate_chunk(self, chunk_path, cs_addr):
        try:
            cs = ServerProxy(cs_addr)
            print("Replicate", chunk_path, 'to', cs_addr)
            chunk = self.get_chunk(chunk_path)
            cs.upload_chunk(chunk_path, chunk)
            print("File", chunk_path, "has replicated to", cs_addr)
            return "ok"
        except Exception as e:
            print("Replication of", chunk_path, 'failed:', str(e))

    def chunk_filename(self, chunk_path):
        if chunk_path[0] == '/':
            return os.path.join(self.local_fs_root, chunk_path[1:])
        else:
            return os.path.join(self.local_fs_root, chunk_path)

    #doesn't work
    def calculate_checksum(self, data):
        # Implement checksum calculation logic (e.g., CRC32)
        return "mock_checksum"
     
    #not required
    def handle_client(self, client_socket):
        try:
            data = client_socket.recv(1024)
            command, *args = data.decode().split()

            if command == "upload":
                filename = args[0]
                file_size = int(args[1])
                checksum = args[2]

                with self.lock:
                    if self.used_storage + file_size <= self.storage_capacity:
                        # Simulate receiving file content
                        file_content = client_socket.recv(file_size)
                        
                        received_checksum = self.calculate_checksum(file_content)
                        if received_checksum == checksum:
                            # Simulate storing the file on the Data Node
                            self.used_storage += file_size
                            file_path = os.path.join('data', filename)
                            with open(file_path, 'wb') as file:
                                file.write(file_content)
                                logging.info(f"File '{filename}' stored successfully on Data Node.")
                        else:
                            logging.error(f"Checksum mismatch. File upload failed.")
                    else:
                        logging.error(f"Insufficient storage space for '{filename}'. Upload failed.")
            # Implement other commands as needed
        except Exception as e:
            logging.error(f"Error handling client: {str(e)}")
        finally:
            client_socket.close()    

    @staticmethod
    def make_sure_path_exists(path):
        try:
            os.makedirs(path)
        except OSError as exception:
            if exception.errno != errno.EEXIST:
                raise


            
# args: host and port: localhost 9999
if __name__ == '__main__':
    host = socket.gethostbyname(socket.gethostname())
    port = 9000

    addr = 'http://' + host + ":" + str(port)
    if not os.getenv('YAD_NS'):
        ns_addr = 'http://localhost:8888'
    else:
        ns_addr = os.environ['YAD_NS']

    cs = ChunkServer(addr, ns_addr)
    cs.start()

    server = SimpleXMLRPCServer((host, port))
    server.register_introspection_functions()
    server.register_instance(cs)
    server.serve_forever()


class DataNode:
    def __init__(self, data_node_address, storage_capacity=100):
        self.data_node_address = data_node_address
        self.storage_capacity = storage_capacity
        self.used_storage = 0
        self.lock = threading.Lock()

        # Wrap the socket with SSL for secure communication
        self.server_socket = ssl.wrap_socket(
            socket.socket(socket.AF_INET, socket.SOCK_STREAM),
            server_side=True,
            certfile="/home/skanda/BigD/YADFS/ssl/server.crt",  # Adjust the path if needed
            keyfile="/home/skanda/BigD/YADFS/ssl/server.key",   # Adjust the path if needed
        )
        self.server_socket.bind(self.data_node_address)
        self.server_socket.listen(5)

        # Start the DataNode server in a separate thread
        data_node_thread = threading.Thread(target=self.start, daemon=True)
        data_node_thread.start()

    def start(self):
        logging.info(f"Data Node {self.data_node_address} listening for connections...")
        while True:
            client_socket, addr = self.server_socket.accept()
            logging.info(f"Accepted connection from {addr}")
            threading.Thread(target=self.handle_client, args=(client_socket,)).start()

    def handle_client(self, client_socket):
        try:
            data = client_socket.recv(1024)
            command, *args = data.decode().split()

            if command == "upload":
                filename = args[0]
                file_size = int(args[1])
                checksum = args[2]

                with self.lock:
                    if self.used_storage + file_size <= self.storage_capacity:
                        # Simulate receiving file content
                        file_content = client_socket.recv(file_size)
                        
                        received_checksum = self.calculate_checksum(file_content)
                        if received_checksum == checksum:
                            # Simulate storing the file on the Data Node
                            self.used_storage += file_size
                            file_path = os.path.join('data', filename)
                            with open(file_path, 'wb') as file:
                                file.write(file_content)
                                logging.info(f"File '{filename}' stored successfully on Data Node.")
                        else:
                            logging.error(f"Checksum mismatch. File upload failed.")
                    else:
                        logging.error(f"Insufficient storage space for '{filename}'. Upload failed.")
            # Implement other commands as needed
        except Exception as e:
            logging.error(f"Error handling client: {str(e)}")
        finally:
            client_socket.close()

    def calculate_checksum(self, data):
        # Implement checksum calculation logic (e.g., CRC32)
        return "mock_checksum"