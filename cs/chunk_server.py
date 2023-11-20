#!/usr/bin/env python3.11
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.client import ServerProxy
import sys
import os
import errno
import time
import socket
import _thread
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
    # Cache frequently accessed chunks in memory or a faster storage medium
    # to improve access performance
    # ...

    # Simulate managing the cached chunks and evicting them when necessary
    # ...

    
    def upload_chunk(self, chunk_path, chunk):
        print('Upload file', chunk_path)
        local_path = self.chunk_filename(chunk_path)
        ldir = os.path.dirname(local_path)
        self.make_sure_path_exists(ldir)
        with open(local_path, "w") as f:
            f.write(chunk)
            return {'status': Status.ok}

    @staticmethod
    def check_replication_level(chunk_path):
    """
    Checks the replication level of the given chunk.
    :param chunk_path: The path to the chunk.
    :return: The replication level of the chunk.
    """

    # Get the replicas of the chunk from the namespace server
    replicas = ns.get_replicas(chunk_path)

    # Check the replication level
    if len(replicas) < 3:
        print("Chunk", chunk_path, "has replication level", len(replicas), "below the minimum of 3.")

    # Simulate replicating the chunk to another datanode
    if len(replicas) < 3:
        other_cs = random.choice(cs_addrs)
        if other_cs != cs_addr:
            print("Replicating", chunk_path, "to", other_cs)
            other_cs.replicate_chunk(chunk_path, cs_addr)
            
            
    def get_chunk(self, chunk_path):
        local_path = self.chunk_filename(chunk_path)
        with open(local_path, "r") as f:
            data = f.read()
        return data
        
    def manage_chunk_versions(chunk_path):
    # Get the list of versions for the chunk
    versions = ns.get_chunk_versions(chunk_path)

    # Simulate deleting old versions of the chunk
    for version in versions:
        if version != "latest":
            version_path = self.chunk_filename(chunk_path + "." + version)
            if os.path.exists(version_path):
                os.remove(version_path)
                print("Deleted old version", version_path)


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

    @staticmethod
    def make_sure_path_exists(path):
        try:
            os.makedirs(path)
        except OSError as exception:
            if exception.errno != errno.EEXIST:
                raise
    @staticmethod
    def check_chunk_health():
    """
    Checks the health of the chunks on the datanode.
    """

    # Check for any chunks that have been marked as dead by the namespace server
    dead_chunks = ns.get_dead_chunks()

    # Simulate deleting dead chunks
    for dead_chunk in dead_chunks:
        if os.path.exists(dead_chunk):
            os.remove(dead_chunk)
            print("Deleted dead chunk", dead_chunk)
            
    def monitor_disk_usage():
    # Get the disk usage of the datanode's storage
    disk_usage = psutil.disk_usage(self.local_fs_root)

    # Check if the disk usage is approaching the threshold
    if disk_usage.percent > 80:
        print("Disk usage is approaching the threshold:", disk_usage.percent, "%")

        # Simulate alerting the namespace server about low disk space
        self.ns.report_low_disk_space(self.addr)
        
        
    def handle_chunk_access_logs(chunk_path):
    # Get the access logs for the chunk
    logs = ns.get_chunk_access_logs(chunk_path)

    # Simulate analyzing access logs to identify usage patterns
    for log in logs:
        print("Chunk", chunk_path, "was accessed by", log.client_addr, "at", log.timestamp)

        # Simulate taking actions based on access patterns
        # ...

            
# args: host and port: localhost 9999
if __name__ == '__main__':
    host = socket.gethostbyname(socket.gethostname())
    port = 9999

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



def check_replication_level(chunk_path):
    """
    Checks the replication level of the given chunk.
    :param chunk_path: The path to the chunk.
    :return: The replication level of the chunk.
    """

    # Get the replicas of the chunk from the namespace server
    replicas = ns.get_replicas(chunk_path)

    # Check the replication level
    if len(replicas) < 3:
        print("Chunk", chunk_path, "has replication level", len(replicas), "below the minimum of 3.")

    # Simulate replicating the chunk to another datanode
    if len(replicas) < 3:
        other_cs = random.choice(cs_addrs)
        if other_cs != cs_addr:
            print("Replicating", chunk_path, "to", other_cs)
            other_cs.replicate_chunk(chunk_path, cs_addr)

