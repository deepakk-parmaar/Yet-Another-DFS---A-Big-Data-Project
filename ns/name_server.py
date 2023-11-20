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

    def distribute_chunk_replicas(chunk_path, replicas):
    # Simulate distributing chunk replicas to different datanodes
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
        
        
    def balance_chunk_distribution():
        pass
        # Simulate balancing chunk distribution across datanodes
        # ...
        # Get a list of all datanodes and their chunk loads
        datanode_loads = {}
        for datanode_addr in datanodes:
            try:
                datanode = ServerProxy(datanode_addr)
                datanode_loads[datanode_addr] = datanode.get_chunk_load()
            except Exception as e:
                print(f"Failed to get chunk load from datanode {datanode_addr}: {str(e)}")

        # Identify datanodes with high chunk loads
        high_load_datanodes = []
        for datanode_addr, chunk_load in datanode_loads.items():
            if chunk_load > average_chunk_load:
                high_load_datanodes.append(datanode_addr)

        # Redistribute chunks from high-load datanodes to low-load datanodes
        for datanode_addr in high_load_datanodes:
            chunks_to_redistribute = datanode.get_chunks_to_redistribute()
            for chunk_path, chunk_data in chunks_to_redistribute.items():
                # Find low-load datanodes to receive the chunk
                low_load_datanodes = list(datanode_loads.keys())
                while True:
                    low_load_datanode = random.choice(low_load_datanodes)
                    if datanode_loads[low_load_datanode] < average_chunk_load:
                        break

                # Distribute the chunk to the chosen low-load datanode
                try:
                    low_load_datanode = ServerProxy(low_load_datanode)
                    low_load_datanode.upload_chunk(chunk_path, chunk_data)
                except Exception as e:
                    print(f"Failed to distribute chunk {chunk_path} to {low_load_datanode}: {str(e)}")


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

    def balance_chunk_distribution():
        # Simulate balancing chunk distribution across datanodes
        # ...

        # Get a list of all datanodes and their chunk loads
        datanode_loads = {}
        for datanode_addr in datanodes:
            try:
                datanode = ServerProxy(datanode_addr)
                datanode_loads[datanode_addr] = datanode.get_chunk_load()
            except Exception as e:
                print(f"Failed to get chunk load from datanode {datanode_addr}: {str(e)}")

        # Identify datanodes with high chunk loads
        high_load_datanodes = []
        for datanode_addr, chunk_load in datanode_loads.items():
            if chunk_load > average_chunk_load:
                high_load_datanodes.append(datanode_addr)

        # Redistribute chunks from high-load datanodes to low-load datanodes
        for datanode_addr in high_load_datanodes:
            chunks_to_redistribute = datanode.get_chunks_to_redistribute()
            for chunk_path, chunk_data in chunks_to_redistribute.items():
                # Find low-load datanodes to receive the chunk
                low_load_datanodes = list(datanode_loads.keys())
                while True:
                    low_load_datanode = random.choice(low_load_datanodes)
                    if datanode_loads[low_load_datanode] < average_chunk_load:
                        break

                # Distribute the chunk to the chosen low-load datanode
                try:
                    low_load_datanode = ServerProxy(low_load_datanode)
                    low_load_datanode.upload_chunk(chunk_path, chunk_data)
                except Exception as e:
                    print(f"Failed to distribute chunk {chunk_path} to {low_load_datanode}: {str(e)}")


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

    def monitor_datanode_health():
        # Simulate monitoring the health of datanodes and identifying unresponsive nodes
        # ...

        # Periodically check the status of all datanodes
        while True:
            unresponsive_datanodes = []
            for datanode_addr in datanodes:
                try:
                    datanode = ServerProxy(datanode_addr)
                    datanode.heartbeat()
                except Exception as e:
                    unresponsive_datanodes.append(datanode_addr)

            # Mark unresponsive datanodes as failed
            for unresponsive_datanode_addr in unresponsive_datanodes:
                print(f"Datanode {unresponsive_datanode_addr} is unresponsive")
                # TODO: Implement mechanism to handle failed datanodes (replicate chunks, etc.)

            time.sleep(heartbeat_interval)  # Check datanode health every heartbeat interval

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

    def balance_chunk_distribution():
        # Simulate balancing chunk distribution across datanodes
        # ...

        # Get a list of all datanodes and their chunk loads
        datanode_loads = {}
        for datanode_addr in datanodes:
            try:
                datanode = ServerProxy(datanode_addr)
                datanode_loads[datanode_addr] = datanode.get_chunk_load()
            except Exception as e:
                print(f"Failed to get chunk load from datanode {datanode_addr}: {str(e)}")
                # TODO: Handle datanode failures gracefully, such as removing them from the datanode list

        # Calculate the average chunk load
        total_chunks = sum(datanode_loads.values())
        average_chunk_load = total_chunks / len(datanode_loads)

        # Identify datanodes with high chunk loads
        high_load_datanodes = []
        for datanode_addr, chunk_load in datanode_loads.items():
            if chunk_load > average_chunk_load:
                high_load_datanodes.append(datanode_addr)

        # Redistribute chunks from high-load datanodes to low-load datanodes
        for datanode_addr in high_load_datanodes:
            chunks_to_redistribute = datanode.get_chunks_to_redistribute()
            for chunk_path, chunk_data in chunks_to_redistribute.items():
                # Find low-load datanodes to receive the chunk
                low_load_datanodes = list(datanode_loads.keys())
                while True:
                    low_load_datanode = random.choice(low_load_datanodes)
                    if datanode_loads[low_load_datanode] < average_chunk_load:
                        break

                # Distribute the chunk to the chosen low-load datanode
                try:
                    low_load_datanode = ServerProxy(low_load_datanode)
                    low_load_datanode.upload_chunk(chunk_path, chunk_data)
                    print(f"Redistributing chunk {chunk_path} from {datanode_addr} to {low_load_datanode}")
                except Exception as e:
                    print(f"Failed to distribute chunk {chunk_path} to {low_load_datanode}: {str(e)}")
                    # TODO: Handle redistribution failures gracefully


    def put_in_queue(self, path, existing_cs):
        item = (path, existing_cs)
        self.queue.put(item)

    def _replicate_worker(self):
        while self.on:
            item = self.queue.get()
            self.replicate(item)

    def monitor_datanode_health():
        # Simulate monitoring the health of datanodes and identifying unresponsive nodes
        # ...

        # Periodically check the status of all datanodes
        while True:
            unresponsive_datanodes = []
            for datanode_addr in datanodes:
                try:
                    datanode = ServerProxy(datanode_addr)
                    datanode.heartbeat()
                except Exception as e:
                    unresponsive_datanodes.append(datanode_addr)

            # Mark unresponsive datanodes as failed
            for unresponsive_datanode_addr in unresponsive_datanodes:
                print(f"Datanode {unresponsive_datanode_addr} is unresponsive")
                # TODO: Implement mechanism to handle failed datanodes (replicate chunks, remove from datanode list, etc.)

            time.sleep(heartbeat_interval)  # Check datanode health every heartbeat interval


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

    def emergency_replication(self):
        print('Start emergency replication for files from')
        self.traverse_replication(self.ns.root)

    def handle_chunk_errors(chunk_path):
        # Check for any errors associated with the chunk
        errors = ns.get_chunk_errors(chunk_path)

        # Simulate repairing corrupted chunks
        for error in errors:
            if error.type == "Corrupted":
                print("Chunk", chunk_path, "is corrupted. Attempting to repair...")

                # Simulate repairing the corrupted chunk
                try:
                    # Read the corrupted chunk data
                    with open(chunk_path, "rb") as f:
                        corrupted_data = f.read()

                    # Perform error correction or data recovery techniques
                    # ...

                    # Write the repaired data to the chunk
                    with open(chunk_path, "wb") as f:
                        f.write(repaired_data)

                    print("Chunk", chunk_path, "has been repaired.")
                except Exception as e:
                    print("Failed to repair chunk", chunk_path, ". Marking it as dead.")

                    # Simulate marking the chunk as dead
                    ns.mark_chunk_dead(chunk_path)


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

    def manage_chunk_versions(chunk_path):
        # Get the list of versions for the chunk
        versions = ns.get_chunk_versions(chunk_path)

        # Simulate deleting old versions of the chunk
        for version in versions:
            if version != "latest":
                version_path = os.path.join(self.local_fs_root, chunk_path + "." + version)
                if os.path.exists(version_path):
                    os.remove(version_path)
                    print("Deleted old version", version_path)

        # Simulate keeping a limited number of recent versions for backup or archival purposes
        # ...

class NameNode:
    def __init__(self, dump_on=True, dump_path="./name_server.yml", cs_timeout=2):
        self.root = FileNode('/', NodeType.directory)
        self.dump_on = dump_on
        self.dump_path = dump_path
        self.cs_timeout = cs_timeout
        self.cs = {}
        self.repl = Replicator(self)

    def start(self):
        self._load_dump()
        self.repl.start()

    def perform_background_data_integrity_checks():
        # Regularly check for any inconsistencies or errors in the stored chunks
        while True:
            # Simulate scanning chunks for inconsistencies
            for chunk_path in os.listdir(self.local_fs_root):
                chunk_data = self.get_chunk(chunk_path)

                # Verify the integrity of the chunk data using checksums or other techniques
                # ...

                if not check_chunk_integrity(chunk_data):
                    print("Chunk", chunk_path, "has data integrity issues.")

                    # Simulate marking the chunk as corrupted and notifying the namenode
                    # ...

            time.sleep(integrity_check_interval)  # Check chunk integrity every integrity_check_interval

    def _load_dump(self):
        if os.path.isfile(self.dump_path):
            print("Trying to read the file tree from the dump file", self.dump_path)
            with open(self.dump_path) as f:
                self.root = yaml.load(f,Loader=yaml.Loader)
            print("File tree has been loaded from the dump file")
        else:
            print("No dump file detected")

    def monitor_chunk_storage_usage():
        # Simulate tracking the storage usage of the datanode's storage
        disk_usage = psutil.disk_usage(self.local_fs_root)

        # Generate alerts or notifications if storage usage reaches a certain threshold
        if disk_usage.percent > storage_usage_threshold:
            print("Datanode storage usage is approaching the threshold:", disk_usage.percent, "%")

            # Simulate alerting the namenode about low disk space
            self.ns.report_low_disk_space(self.addr)

        # Implement mechanisms to optimize storage usage, such as purging old or unused chunks
        # ...

    
    
    
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

    def _select_available_cs(self, ignore_cs=None):
        if ignore_cs is None:
            ignore_cs = []

        live = [cs_name for cs_name in self.cs if self._is_alive_cs(cs_name) and cs_name not in ignore_cs]

        if len(live) == 0:
            return None

        i = random.randint(0, len(live) - 1)
        return live[i]

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

    def perform_namespace_garbage_collection():
        # Simulate periodically removing orphaned files and unused chunks from the namespace
        orphaned_files = []
        unused_chunks = []

        # Identify files that have no associated chunks or are no longer referenced by any users
        for file_path, file_metadata in ns.get_all_file_metadata().items():
            if not file_metadata.chunks:
                orphaned_files.append(file_path)

        # Identify chunks that are not associated with any files
        for chunk_path in ns.get_all_chunks():
            if chunk_path not in ns.get_all_file_chunks().values():
                unused_chunks.append(chunk_path)

        # Mark orphaned files and unused chunks as garbage and schedule them for deletion
        for file_path in orphaned_files:
            ns.mark_file_as_garbage(file_path)

        for chunk_path in unused_chunks:
            ns.mark_chunk_as_garbage(chunk_path)

        # Perform cleanup operations to remove garbage from the namespace and datanodes
        # ...


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

    def implement_namespace_backup_and_recovery():
        # Simulate creating regular backups of the namespace to ensure data integrity and recoverability
        backup_interval = 60 * 60  # Create backups every hour

        while True:
            # Generate a timestamp for the backup
            timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

            # Create a backup of the namespace data
            backup_data = ns.get_all_file_metadata()

            # Store the backup data to a separate storage location
            backup_filename = f"namespace_backup_{timestamp}.json"
            with open(backup_filename, "w") as f:
                json.dump(backup_data, f)

            print("Created namespace backup:", backup_filename)

            time.sleep(backup_interval)  # Create backups every backup_interval


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

    def monitor_chunk_storage_usage():
        # Simulate tracking the storage usage of the datanode's storage
        disk_usage = psutil.disk_usage(self.local_fs_root)

        # Generate alerts or notifications if storage usage reaches a certain threshold
        if disk_usage.percent > storage_usage_threshold:
            print("Datanode storage usage is approaching the threshold:", disk_usage.percent, "%")

            # Simulate alerting the namenode about low disk space
            self.ns.report_low_disk_space(self.addr)

        # Implement mechanisms to optimize storage usage, such as purging old or unused chunks
        # ...


    def make_directory(self, path):
        d = self.root.find_path(path)

        if d is not None:
            return {'status': Status.already_exists}

        d = self.root.create_dir(path)
        if d == "Error":
            return {'status': Status.error}

        return {'status': Status.ok}

    def handle_chunk_access_logs(chunk_path):
        # Get the access logs for the chunk
        logs = ns.get_chunk_access_logs(chunk_path)

        # Simulate analyzing access logs to identify usage patterns
        for log in logs:
            print("Chunk", chunk_path, "was accessed by", log.client_addr, "at", log.timestamp)

            # Simulate taking actions based on access patterns, such as caching frequently accessed chunks
            # ...


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

    def perform_namespace_consistency_checks():
        # Regularly check for any inconsistencies or errors in the namespace
        while True:
            # Simulate scanning the namespace for inconsistencies
            inconsistencies = []

            # Check for orphaned chunks (chunks not associated with any files)
            # ...

            # Check for duplicate file entries with the same name but different metadata
            # ...

            # Simulate correcting any detected inconsistencies
            # ...

            if inconsistencies:
                print("Inconsistencies detected in the namespace:", inconsistencies)
            else:
                print("Namespace consistency check: No inconsistencies found.")

            time.sleep(consistency_check_interval)  # Check namespace consistency every consistency_check_interval


    def size_of(self, path):
        i = self.root.find_path(path)
        if i is None:
            return {'status': Status.not_found, 'size': 0}

        return {'status': Status.ok, 'size': i.size}
    def manage_file_metadata(file_path, metadata):
        # Update the file metadata in the namenode's storage
        # ...

        # Simulate notifying datanodes about changes to file metadata
        # ...
        for datanode_addr in datanodes:
            try:
                datanode = ServerProxy(datanode_addr)
                datanode.update_file_metadata(file_path, metadata)
            except Exception as e:
                print(f"Failed to notify datanode {datanode_addr} about file metadata update: {str(e)}")

        
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
