#!/usr/bin/env python3.11
import os
import errno
from xmlrpc.client import ServerProxy
import ssl
import socket

import sys
# from os.path import dirname
# sys.path.append(dirname(dirname(__file__)))
# from utils.enums import Status
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


class Client:
    def __init__(self, ns_addr=None):
        self.chunk_servers = []
        if ns_addr is not None:
            os.environ['YAD_NS'] = ns_addr
        elif not os.getenv('YAD_NS'):
            os.environ['YAD_NS'] = 'http://localhost:8888'

        self.ns = ServerProxy(os.environ['YAD_NS'])

    def list(self, path):
        return self.ns.list(path)

    def list_dir(self, dir_path):
        return self.ns.list_directory(dir_path)
    
    def list_file(self, file_path):
        return self.ns.list_file(file_path)

    def create_dir(self, path):
        return self.ns.make_directory(path)
    


    def delete_dir(self, path):
        return self.ns.delete_dir(path)
    
    def delete_file(self, path):
        return self.ns.delete_file(path)

    def create_file(self, path, remote_path):
        fn = path.split("/")[-1]
        remote_filepath = os.path.join(remote_path, fn)

        if not os.path.isfile(path):
            return {'status': Status.not_found}

        with open(path, 'r') as fr:
            data = fr.read()

        return self._save_file_to_dfs(data, remote_filepath)
    
    
    def _save_file_to_dfs(self, content, remote_filepath):
        r = self._get_cs(remote_filepath)
        if not r['status'] == Status.ok:
            return r
        cs_addr = r['cs']
        cs = ServerProxy(cs_addr)

        chunks = self.split_file(content)
        data = {}
        data['path'] = remote_filepath
        data['size'] = len(content)
        data['chunks'] = {}
        for count, chunk in enumerate(chunks):
            cs.upload_chunk(remote_filepath + '_{0}'.format(str(count)), chunk)
            data['chunks'][remote_filepath + '_' + str(count)] = cs_addr

        return self.ns.create_file(data)
    
    def upload_file(self, path, remote_path):
        fn = path.split("/")[-1]
        remote_filepath = os.path.join(remote_path, fn)

        if not os.path.isfile(path):
            return {'status': Status.not_found}

        with open(path, 'r') as fr:
            data = fr.read()

        return self._save_file_to_dfs(data, remote_filepath)

    def delete(self, path):
        return self.ns.delete(path)
    
    def delete_file(self, path):
        return self.ns.delete_file(path)

    def download_file(self, path, dst_path):
        result, content = self.get_file_content(path)
        if result != Status.ok:
            return {'status': result}

        fn = path.split("/")[-1]
        self.make_sure_path_exists(dst_path)
        file_path = os.path.join(dst_path, fn)
        with open(file_path, "w") as f:
            f.write(content)
            return {'status': Status.ok}
        
    def download_dir(self, path, dst_path):
        result, content = self.get_file_content(path)
        if result != Status.ok:
            return {'status': result}

        fn = path.split("/")[-1]
        self.make_sure_path_exists(dst_path)
        file_path = os.path.join(dst_path, fn)
        with open(file_path, "w") as f:
            f.write(content)
            return {'status': Status.ok}

    def get_file_content(self, path):
        info = self.ns.get_file_info(path)
        if info['status'] != Status.ok:
            return info['status'], None

        chunks = info['chunks']
        content = ""
        data = {}
        for chunk, addr in chunks.items():
            cs = ServerProxy(addr)
            chunk_data = cs.get_chunk(chunk)
            index = int(chunk.split("_")[-1])
            data[index] = chunk_data

        i = 0
        while i < len(data):
            content += data[i]
            i += 1

        return Status.ok, content



    def get_file_info(self, path):
        info = self.ns.get_file_info(path)
        if info['status'] != Status.ok:
            return info['status'], None
        
        return Status.ok, info
    


    def get_chunk(self, path, chunk_id):
        info = self.ns.get_file_info(path)
        if info['status'] != Status.ok:
            return info['status'], None
        
        chunks = info['chunks']
        for chunk, addr in chunks.items():
            if int(chunk.split("_")[-1]) == chunk_id:
                cs = ServerProxy(addr)
                chunk_data = cs.get_chunk(chunk)
                return Status.ok, chunk_data
    
    def get_chunk_info(self, path, chunk_id):
        info = self.ns.get_file_info(path)
        if info['status'] != Status.ok:
            return info['status'], None
        
        chunks = info['chunks']
        for chunk, addr in chunks.items():
            if int(chunk.split("_")[-1]) == chunk_id:
                return Status.ok, chunk

    def path_status(self, path):
        return self.ns.get_file_info(path)
    
    def get_file_info_(self, path):
        return self.ns.get_file_info(path)

    def _get_cs(self, path):
        result = self.ns.get_cs(path)
        print(result)
        if 'cs' not in result:
            return {'status': Status.error, 'cs': None}
        return {'status': Status.ok, 'cs': result['cs']}
    
    def get_cs(self, path):
        return self._get_cs(path)

    def get_chunk(self, path):
        r_index = path.rindex('_')
        f_path = path[:r_index]

        info = self.ns.get_file_info(f_path)
        for chunk, addr in info['chunks'].items():
            if chunk == path:
                cs = ServerProxy(addr)
                return {'status': Status.ok, 'data': cs.get_chunk(chunk)}

        return {'status': Status.not_found}

    def get_chunk_info_2(self, path):
        r_index = path.rindex('_')
        f_path = path[:r_index]

        info = self.ns.get_file_info(f_path)
        for chunk, addr in info['chunks'].items():
            if chunk == path:
                return {'status': Status.ok, 'data': chunk}

        return {'status': Status.not_found}
    
    def download_to(self, v_path, l_path):
        st, data = self.get_file_content(v_path)
        os.makedirs(os.path.dirname(l_path), exist_ok=True)
        with open(l_path, "w") as f:
            f.write(data)

        return {'status': Status.ok}
    

    def save(self, data, path):
        st = self._save_file_to_dfs(data, path)
        return {'status': st}

    def download_to_(self, v_path, l_path):
        st, data = self.get_file_content(v_path)
        os.makedirs(os.path.dirname(l_path), exist_ok=True)
        with open(l_path, "w") as f:
            f.write(data)

        return {'status': Status.ok}
    
    def save_(self, data, path):

        st = self._save_file_to_dfs(data, path)
        return {'status': st}
    

    @staticmethod
    def split_file(data, chunksize=1024):
        chunks = []
        while len(data) >= chunksize:
            i = chunksize
            while not data[i]==' ':
                if i == 0:
                    i = chunksize
                    break
                else:
                    i -= 1
            chunks.append(data[:i])
            data = data[i:]
        chunks.append(data)
        return chunks

    
    @staticmethod
    def write_combined_file(filename, chunks):
        if os.path.isfile(filename):
            return Status.already_exists

        with open(filename, 'x') as fw:
            for chunk in chunks:
                fw.write(chunk)

    @staticmethod
    def make_sure_path_exists(path):
        try:
            os.makedirs(path)
        except OSError as exception:
            if exception.errno != errno.EEXIST:
                raise

class client:
    def __init__(self, name_node_address):
        self.name_node_address = name_node_address

    def authenticate(self):
        # Implement authentication logic here
        # Example: send a challenge and expect a valid response from the server
        # Simulating a simple authentication challenge-response
        with socket.create_connection(self.name_node_address) as client_socket:
            client_socket = ssl.wrap_socket(client_socket, ssl_version=ssl.PROTOCOL_TLSv1_2)
            client_socket.send(b"Challenge")
            response = client_socket.recv(1024)
            if response == b"ValidResponse":
                print("Authentication successful.")
                return True
            else:
                print("Authentication failed.")
                return False

    def upload_file(self, filename):
        if self.authenticate():
            with socket.create_connection(self.name_node_address) as client_socket:
                client_socket = ssl.wrap_socket(client_socket, ssl_version=ssl.PROTOCOL_TLSv1_2)
                with open(filename, 'rb') as file:
                    file_content = file.read()
                    file_size = len(file_content)
                    checksum = "mock_checksum"  # Calculate the actual checksum

                    # Send upload command to NameNode
                    upload_command = f"upload {filename} {file_size} {checksum}"
                    client_socket.send(upload_command.encode())

                    # Simulate sending file content to NameNode
                    client_socket.sendall(file_content)

                print(f"File '{filename}' uploaded successfully to DFS.")
