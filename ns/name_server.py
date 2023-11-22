#!/usr/bin/env python3.11
import os
import random
import sys
import yaml
import _thread
import socket
from datetime import datetime
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.client import ServerProxy
import time
from queue import Queue
import threading
import json
import ssl

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


class FileNode:
    def __init__(self, name, node_type):
        self.name = name
        self.parent = None
        self.children = []
        self.type = node_type
        self._size = 0
        self.date = time.time()
        self.chunks = {}

    @property
    def is_root(self):
        return self.parent is None

    @property
    def size(self):
        if self.type == NodeType.directory:
            return sum(c.size for c in self.children)

        return self._size
    
    #not needed, covered elsewhere
    def distribute_chunk_replicas(chunk_path, replicas):
        for replica in replicas:
            datanode_addr = replica.get('addr')
            try:
                datanode = ServerProxy(datanode_addr)
                print(f"Distributing chunk {chunk_path} to datanode {datanode_addr}")
                datanode.upload_chunk(chunk_path, replica.get('data'))
            except Exception as e:
                print(f"Failed to replicate chunk {chunk_path} to {datanode_addr}: {str(e)}")

    
    @size.setter
    def size(self, value):
        self._size = value

    def add_child(self, child):
        self.children.append(child)
        child.parent = self

    #not working (extra)    
    def find_file_by_extension(self, extension):
        for child in self.children:
            if child.type == NodeType.file and child.name.lower().endswith(extension):
                return child
        return None

    def find_path(self, path):
        if path == self.name:
            return self

        ch_name, ch_path = self._extract_child_path_and_name(path)

        for child in self.children:
            if child.name == ch_name:
                return child.find_path(ch_path)

        return None

    @staticmethod
    def _extract_child_path_and_name(path):
        path = path.strip('/')
        next_sep = path.find('/')

        if next_sep == -1:
            ch_name = path
            ch_path = path
        else:
            ch_name = path[0:next_sep]
            ch_path = path[next_sep + 1:]
        return ch_name, ch_path

    #this logging is done on terminal itself. no need for new files
    def update_file_last_modified(self, path):
        try:
            modification_time = os.path.getmtime(path)
            self.find_path(path).date = modification_time
            return True
        except Exception as e:
            print("Error updating file last modified time:", e)
            return False

    #Not working
    def rename_file(self, path, new_name):
        try:
            os.rename(path, new_name)
            self.find_path(path).name = new_name
            return True
        except Exception as e:
            print("Error renaming file:", e)
            return False

    @staticmethod
    def _extract_file_name_and_file_dir(path):
        path = path.strip('/')
        sep = path.rfind('/')

        if sep == -1:
            f_name = path
            f_dir = None
        else:
            f_name = path[sep + 1: len(path)]
            f_dir = path[: sep]
        return f_name, f_dir

    #can be done manually
    def move_file(self, path, new_path):
        try:
            shutil.move(path, new_path)
            self.find_path(path).name = new_name
            self.find_path(new_path).add_child(file_node)
            return True
        except Exception as e:
            print("Error moving file:", e)
            return False

    def create_dir(self, path):
        dirs = path.strip('/').split('/')
        curr_dir = self
        i = 0
        while i < len(dirs):
            d_name = dirs[i]

            directory = next((x for x in curr_dir.children if x.name == d_name), None)
            if directory is None:
                directory = FileNode(d_name, NodeType.directory)
                curr_dir.add_child(directory)

            elif directory.type == NodeType.file:
                print("Can't create directory because of the wrong file name " + d_name)
                return "Error"
            curr_dir = directory
            i += 1

        return curr_dir

    def create_file(self, path):
        f_name, f_dir = self._extract_file_name_and_file_dir(path)

        if f_dir is not None:
            directory = self.create_dir(f_dir)

            if directory == "Error":
                return 'Error'
        else:
            directory = self

        file = FileNode(f_name, NodeType.file)
        directory.add_child(file)
        return file

    #
    def get_file_permissions(self, path):
        try:
            stat_info = os.stat(path)
            return stat_info.st_mode
        except Exception as e:
            print("Error getting file permissions:", e)
            return -1

    def find_file_by_chunk(self, path):

        i = path.rfind('_')
        if i is None:
            print("Incorrect chunk path", path)
            return Status.error

        file_name = path[:i]
        return self.find_path(file_name)

    def delete(self):
        if not self.is_root:
            self.parent.children.remove(self)
            self.parent = None

    def append_to_file(self, path, content):
        try:
            with open(path, 'a') as file:
                file.write(content)
            self.find_path(path)._size += len(content)
            return True
        except Exception as e:
            print("Error appending to file:", e)
            return False

    def get_full_path(self):
        if self.is_root:
            return ''
        else:
            return self.parent.get_full_path() + '/' + self.name

    def get_full_dir_path(self):
        if self.is_root:
            return ''

        path = self.parent.get_full_dir_path()
        if self.type == NodeType.directory:
            path += '/' + self.name

        return path

class Replicator:
    def __init__(self, ns):
        self.ns = ns
        self.queue = Queue()
        self.on = True

    def distribute_chunk_replicas(chunk_path, replicas):
        # Simulate distributing chunk replicas to different datanodes
        for replica in replicas:
            datanode_addr = replica.get('addr')
            try:
                datanode = ServerProxy(datanode_addr)
                datanode.upload_chunk(chunk_path, replica.get('data'))
                print(f"Replicating chunk {chunk_path} to datanode {datanode_addr}")
            except Exception as e:
                print(f"Failed to replicate chunk {chunk_path} to {datanode_addr}: {str(e)}")
                # TODO: Handle replication failures gracefully, such as retrying or notifying the administrator

    
    def start(self):
        print('Start replication workers')
        _thread.start_new_thread(self._replicate_worker, ())
        _thread.start_new_thread(self.server_watcher, ())

    #
    def balance_replication_load(self):
        try:
            _thread.start_new_thread(self._replicate_worker, ())
            return True
        except Exception as e:
            print("Error balancing replication load:", e)
            return False

    def put_in_queue(self, path, existing_cs):
        item = (path, existing_cs)
        self.queue.put(item)

    def _replicate_worker(self):
        while self.on:
            item = self.queue.get()
            self.replicate(item)
    
    #
    def create_dir(self, path):
        dirs = path.strip('/').split('/')
        curr_dir = self
        i = 0
        while i < len(dirs):
            d_name = dirs[i]

            directory = next((x for x in curr_dir.children if x.name == d_name), None)
            if directory is None:
                directory = FileNode(d_name, NodeType.directory)
                curr_dir.add_child(directory)

            elif directory.type == NodeType.file:
                print("Can't create directory because of the wrong file name " + d_name)
                return "Error"
            curr_dir = directory
            i += 1

        return curr_dir

    def replicate(self, item):
        if item is None:
            return

        path = item[0]
        cs_list = item[1]

        alive = [x for x in cs_list if self.ns._is_alive_cs(x)]

        if len(alive) == 0:
            print('There is no live CS for chunk', path)
            return

        if len(alive) >= 2:
            print('File', path, 'is already replicated to more than 2 nodes')
            return

        new_cs = self.ns._select_available_cs(alive)
        if new_cs is None:
            print("Can't find available CS for replication", path)
        else:
            try:
                cl = ServerProxy(alive[0])
                cl.replicate_chunk(path, new_cs)
                print("File", path, "replicated to", new_cs)

                file = self.ns.root.find_file_by_chunk(path)
                if file is not None and path in file.chunks:
                    file.chunks[path].append(new_cs)
                else:
                    print("Can't find file for chunk", path, "after replication")
            except Exception as e:
                print('Error during replication', path, 'to', new_cs, ':', e)

    def server_watcher(self):
        while self.on:
            for cs_name in list(self.ns.cs):
                if not self.ns._is_alive_cs(cs_name):
                    print('CS', cs_name, 'detected as not alive')
                    self.ns.cs.pop(cs_name)
                    _thread.start_new_thread(self.emergency_replication, ())
            time.sleep(1)


    def remove_replica(self, data_node_address): #removes the replicas if needed.
        with self.lock:
            if data_node_address in self.data_nodes:
                self.data_nodes.remove(data_node_address)
                print(f"Data Node {data_node_address} removed.")

    def emergency_replication(self):
        print('Start emergency replication for files from')
        self.traverse_replication(self.ns.root)

    def heartbeat(self, cs_addr): #
        if cs_addr not in self.cs:
            print('Register CS ' + cs_addr)

        self.cs[cs_addr] = datetime.now()
        return {'status': Status.ok}

    def traverse_replication(self, item):
        if item.type == NodeType.file:
            for c in list(item.chunks):
                alive = [x for x in item.chunks[c] if self.ns._is_alive_cs(c)]
                if len(alive) < 2:
                    print('Chunk', c, 'put to replication')
                    self.put_in_queue(c, item.chunks[c])
        else:
            for c in item.children:
                self.traverse_replication(c)


class NameNode:
    def __init__(self, dump_on=True, dump_path="./metadata.yml", cs_timeout=2):
        self.root = FileNode('/', NodeType.directory)
        self.dump_on = dump_on
        self.dump_path = dump_path
        self.cs_timeout = cs_timeout
        self.cs = {}
        self.repl = Replicator(self)

    def start(self):
        self._load_dump()
        self.repl.start()


    def _load_dump(self):
        if os.path.isfile(self.dump_path):
            print("Trying to read the file tree from the dump file", self.dump_path)
            with open(self.dump_path) as f:
                self.root = yaml.load(f,Loader=yaml.Loader)
            print("File tree has been loaded from the dump file")
        else:
            print("No dump file detected")

    def load_metadata(self):
        try:
            with open(self.metadata_path, 'r') as file:
                file_content = file.read()
                if file_content:
                    self.metadata = json.loads(file_content)
                else:
                    self.metadata = {}
        except (FileNotFoundError, json.decoder.JSONDecodeError):
            # Handle the case when the file is not found or not valid JSON
            self.metadata = {}    

    def _dump(self):
        if self.dump_on:
            try:
                with open(self.dump_path, 'w') as outfile:
                    outfile.write(yaml.dump(self.root))
            except Exception as e:
                print("Error dumping file", self.dump_path, e)

    def get_cs(self, path):
        if self.root.find_path(path) is not None:
            print(self.root.find_path(path))
            return {'status': Status.already_exists}
        
        cs = self._select_available_cs()
        if cs is None:
            print("No available chunk servers found")
            return {'status': Status.not_found}

        return {'status': Status.ok, 'cs': cs}

    def get_chunk_server_status(self):
        chunk_server_status = {}
    # Collect health metrics, load metrics, storage utilization, and network performance for each chunk server. returns json instead of yml format. so don't use
        for cs_addr in self.cs:
            try:
                cl = ServerProxy(cs_addr)
                status = cl.get_status()
                resources = cl.get_resources()
                network_metrics = cl.get_network_metrics()

                chunk_server_status[cs_addr] = {'status': status, 'resources': resources, 'network': network_metrics}
            except Exception as e:
                print('Error getting chunk server status:', cs_addr, e)
                chunk_server_status[cs_addr] = {'status': Status.error}

        return {'status': Status.ok, 'chunk_server_status': chunk_server_status}

    def _select_available_cs(self, ignore_cs=None):
        if ignore_cs is None:
            ignore_cs = []

        live = [cs_name for cs_name in self.cs if self._is_alive_cs(cs_name) and cs_name not in ignore_cs]

        if len(live) == 0:
            return None

        i = random.randint(0, len(live) - 1)
        return live[i]

    def search_files(self, query): 
        search_results = []

        for file in self.root.get_all_files():
            if matches_query(file, query):
                search_results.append(file)
        #this has been implemented as ls. use ls instead
        return {'status': Status.ok, 'search_results': search_results}

    def _is_alive_cs(self, cs_addr):
        if cs_addr not in self.cs:
            return False

        last_hb = self.cs[cs_addr]
        now = datetime.now()
        diff = (now - last_hb).total_seconds()
        return diff <= self.cs_timeout

    def create_file(self, data):
        file = self.root.find_path(data['path'])
        print(file)
        if file is not None:
            return {'status': Status.already_exists}

        file = self.root.create_file(data['path'])
        if file == "Error":
            return {'status': Status.error}

        file.size = data['size']
        for k, v in data['chunks'].items():
            file.chunks[k] = [v]
            print('Chunk', k, 'saved on', v)

        for c in file.chunks:
            self.repl.put_in_queue(c, file.chunks[c])

        self._dump()
        print("Created file " + data['path'] + ' of size ' + str(data['size']))

        return {'status': Status.ok}

    #shreya's version
    def handle_chunk_server_failures(self, cs_addr):
        try:
            print('Handling failure of chunk server', cs_addr)
            failed_cs = self.cs.pop(cs_addr)

            for chunk_path, servers in failed_cs.items():
                if len(servers) > 1:
                    print('Replicating chunk', chunk_path, 'to remaining servers:', servers[1:])
                else:
                    new_cs = self._select_available_cs()
                    print('Replicating chunk', chunk_path, 'to new server', new_cs)

            print('Notifying administrators of chunk server failure:', cs_addr)
            self.cs.pop(cs_addr)

        except Exception as e:
            print("Error handling chunk server failure:", e)
            return False

    def delete(self, path):
        print("Delete", path)
        item = self.root.find_path(path)
        if item is None:
            return {'status': Status.not_found}

        if item.is_root:
            return {'status': Status.error}

        item.delete()
        self._dump()
        _thread.start_new_thread(self.delete_from_chunk_servers, (item,))
        return {'status': Status.ok}

    def delete_from_chunk_servers(self, file):
        if file.type == NodeType.directory:
            for c in list(file.children):
                self.delete_from_chunk_servers(c)
        else:
            print('Start delete file', file.get_full_path())
            for f_path, servers in file.chunks.items():
                for cs in servers:
                    try:
                        cl = ServerProxy(cs)
                        print('Send delete', f_path, 'to', cs)
                        cl.delete_chunk(f_path)
                    except:
                        print('Failed to delete', f_path, 'from', cs)


    def get_random_data_node(self):
        return next(iter(self.data_nodes)) # self.data_nodes.status == Status.ok

    def get_file_info(self, path):
        file = self.root.find_path(path)
        if file is None:
            return {'status': Status.not_found}

        chunks = {}
        for c_path, val in file.chunks.items():
            chunks[c_path] = val[0]

        return {'status': Status.ok,
                'type': file.type,
                'path': file.get_full_path(),
                'size': file.size,
                'date': file.date,
                'chunks': chunks}

    def make_directory(self, path):
        d = self.root.find_path(path)

        if d is not None:
            return {'status': Status.already_exists}

        d = self.root.create_dir(path)
        if d == "Error":
            return {'status': Status.error}

        return {'status': Status.ok}

    def resolve_namespace(self, path):
        # Implement logic to resolve file/directory path to physical location
        # For simplicity, assume a flat namespace
        return f"/{path}"

    def ping_data_node(self, data_node_address):
        try:
            with socket.create_connection(data_node_address, timeout=2):
                print(f"Pinging Data Node {data_node_address}")
                return True
        except (socket.timeout, ConnectionRefusedError) as e:
            print(f"Error pinging Data Node {data_node_address}: {e}")
            return False

    def list_directory(self, path):
        print('Request to list directory ' + path)
        directory = self.root.find_path(path)
        if directory is None:
            return {'status': Status.not_found}

        items = {}
        for f in directory.children:
            items[f.name] = self.get_file_info(f.get_full_path())

        result = {'status': Status.ok, 'items': items}
        return result


    def validate_chunk_servers(self):
    # This function attempts to validate the health of chunk servers by checking their status and resource availability.
        try:
            for cs_addr in self.cs:
                cl = ServerProxy(cs_addr)
                status = cl.get_status()
                if status['status'] != Status.ok:
                    print('Chunk server', cs_addr, 'is not healthy:', status)
                    self.cs.pop(cs_addr)
                    continue
                resources = cl.get_resources()
                if resources['cpu'] < 20 or resources['memory'] < 50 or resources['storage'] < 100:
                    print('Chunk server', cs_addr, 'is low on resources:', resources)
            return True
        except Exception as e:
            print("Error validating chunk servers:", e)
            return False

    def size_of(self, path):
        i = self.root.find_path(path)
        if i is None:
            return {'status': Status.not_found, 'size': 0}

        return {'status': Status.ok, 'size': i.size}
        
    def heartbeat(self, cs_addr):
        if cs_addr not in self.cs:
            print('Register CS ' + cs_addr)

        self.cs[cs_addr] = datetime.now()
        return {'status': Status.ok}


if __name__ == '__main__':
    host = socket.gethostbyname(socket.gethostname())
    port = 8888
    ns = NameNode(dump_on=True)
    ns.start()

    server = SimpleXMLRPCServer((host, port), logRequests=False)
    server.register_introspection_functions()
    server.register_instance(ns)
    server.serve_forever()



class namenode:
    def __init__(self):
        self.metadata = {}  # File and directory metadata
        self.data_nodes = set()  # Set of available Data Nodes
        self.replication_factor = 3  # Replication factor
        self.lock = threading.Lock()
        self.metadata_path = '/home/skanda/BigD/YADFS/metadata/metadata.json'

        # Wrap the socket with SSL for secure communication
        self.server_socket = ssl.wrap_socket(
            socket.socket(socket.AF_INET, socket.SOCK_STREAM),
            server_side=True,
            certfile="/home/skanda/BigD/YADFS/ssl/server.crt",
            keyfile="/home/skanda/BigD/YADFS/ssl/server.key",
        )
        self.name_node_address = ('localhost', 5000)
        self.server_socket.bind(self.name_node_address)
        self.server_socket.listen(5)

        # Load existing metadata from a persistent storage
        self.load_metadata()

        # Start a thread to periodically ping Data Nodes
        ping_thread = threading.Thread(target=self.ping_data_nodes, daemon=True)
        ping_thread.start()

    def load_metadata(self):
        try:
            with open(self.metadata_path, 'r') as file:
                file_content = file.read()
                if file_content:
                    self.metadata = json.loads(file_content)
                else:
                    self.metadata = {}
        except (FileNotFoundError, json.decoder.JSONDecodeError):
            # Handle the case when the file is not found or not valid JSON
            self.metadata = {}
    
    def save_metadata(self):
        with open('/home/skanda/BigD/YADFS/metadata/metadata.json', 'w') as file:
            json.dump(self.metadata, file)
            print("Metadata saved successfully.")

    def add_data_node(self, data_node_address):
        with self.lock:
            self.data_nodes.add(data_node_address)
            print(f"Data Node {data_node_address} added.")

    def remove_data_node(self, data_node_address):
        with self.lock:
            if data_node_address in self.data_nodes:
                self.data_nodes.remove(data_node_address)
                print(f"Data Node {data_node_address} removed.")

    def ping_data_nodes(self):
        while True:
            for data_node in list(self.data_nodes):
                if not self.ping_data_node(data_node):
                    print(f"Data Node {data_node} is not responding. Removing from available nodes.")
                    self.remove_data_node(data_node)
            threading.Event().wait(5)  # Ping every 5 seconds

    def ping_data_node(self, data_node_address):
        try:
            with socket.create_connection(data_node_address, timeout=2):
                print(f"Pinging Data Node {data_node_address}")
                return True
        except (socket.timeout, ConnectionRefusedError) as e:
            print(f"Error pinging Data Node {data_node_address}: {e}")
            return False

    def resolve_namespace(self, path):
        # Implement logic to resolve file/directory path to physical location
        # For simplicity, assume a flat namespace
        return f"/{path}"

    def upload_file(self, filename):
        with self.lock:
            # Simulate file splitting and block creation
            blocks = ["block1", "block2", "block3"]
            file_metadata = {
                'filename': filename,
                'blocks': blocks,
                'replicas': {},
            }

            # Replicate blocks to Data Nodes
            for block in blocks:
                replicas = set()
                for _ in range(self.replication_factor):
                    replica_node = self.get_random_data_node()
                    replicas.add(replica_node)
                file_metadata['replicas'][block] = replicas

            # Update metadata
            self.metadata[filename] = file_metadata

            print(f"File '{filename}' uploaded. Metadata: {file_metadata}")
            self.save_metadata()

    def get_random_data_node(self):
        return next(iter(self.data_nodes))