#!/usr/bin/env python3.11
import unittest
import os

from client.client import Client
from ns.name_server import FileNode
from utils.enums import Status, NodeType
from ns.name_server import NameServer

class ClientTests(unittest.TestCase):

    def setUp(self):
        with open('test_small.txt', 'x') as test_small:
            test_small.write(self.content[:12])
        with open('test_large.txt', 'x') as test_large:
            test_large.write(self.content)

    def tearDown(self):
        os.remove('test_small.txt')
        os.remove('test_large.txt')

    def test_split_small_file(self):
        chunks = Client.split_file('test_small.txt')
        self.assertEqual(chunks[0], self.content[:12])
        self.assertEqual(len(chunks), 1)

    def test_split_large_file(self):
        chunks = Client.split_file('test_large.txt')
        self.assertEqual(chunks[1], self.content[1024:])
        self.assertEqual(chunks[0]+chunks[1], self.content)

    content = 'Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.\nSed ut perspiciatis unde omnis iste natus error sit voluptatem accusantium doloremque laudantium, totam rem aperiam, eaque ipsa quae ab illo inventore veritatis et quasi architecto beatae vitae dicta sunt explicabo. Nemo enim ipsam voluptatem quia voluptas sit aspernatur aut odit aut fugit, sed quia consequuntur magni dolores eos qui ratione voluptatem sequi nesciunt. Neque porro quisquam est, qui dolorem ipsum quia dolor sit amet, consectetur, adipisci velit, sed quia non numquam eius modi tempora incidunt ut labore et dolore magnam aliquam quaerat voluptatem. Ut enim ad minima veniam, quis nostrum exercitationem ullam corporis suscipit laboriosam, nisi ut aliquid ex ea commodi consequatur? Quis autem vel eum iure reprehenderit qui in ea voluptate velit esse quam nihil molestiae consequatur, vel illum qui dolorem eum fugiat quo voluptas nulla pariatur?'

if __name__ == '__main__':
    unittest.main()
    

class FileNodeTests(unittest.TestCase):

    def setUp(self):
        super().setUp()
        self.root = FileNode("root", NodeType.directory)

    def test_find_node(self):
        file = FileNode("test.txt", NodeType.file)
        self.root.add_child(file)
        self.assertEqual(file, self.root.find_path("/test.txt"))

    def test_among_several_files(self):
        file = FileNode("test1.txt", NodeType.file)
        self.root.add_child(file)

        file = FileNode("test2.txt", NodeType.file)
        self.root.add_child(file)

        file = FileNode("another_file", NodeType.file)
        self.root.add_child(file)

        self.assertEqual(file, self.root.find_path("/another_file"))

    def test_find_in_sub_folders(self):
        parent = self.root
        folder = FileNode("usr", NodeType.directory)
        parent.add_child(folder)

        parent = folder
        folder = FileNode("bin", NodeType.directory)
        parent.add_child(folder)

        parent = folder
        folder = FileNode("local", NodeType.directory)
        parent.add_child(folder)

        self.assertEqual(folder, self.root.find_path("/usr/bin/local"))

    def test_if_not_found_return_none(self):
        parent = self.root
        folder = FileNode("usr", NodeType.directory)
        parent.add_child(folder)

        parent = folder
        folder = FileNode("bin", NodeType.directory)
        parent.add_child(folder)

        parent = folder
        folder = FileNode("local", NodeType.directory)
        parent.add_child(folder)

        self.assertEqual(None, self.root.find_path("/usr/ans"))

    def test_create_directory(self):
        r = self.root.create_dir("/etc/nginx/log")
        self.assertNotEqual("Error", r)

        dir = self.root.find_path("/etc/nginx/log")
        self.assertEqual("log", dir.name)
        self.assertEqual("nginx", dir.parent.name)
        self.assertEqual("etc", dir.parent.parent.name)

    def test_create_file_dir(self):
        self.root.create_dir("/etc/nginx/")
        f1 = self.root.create_file("/etc/nginx/file_dir1/file_dir2/my_file")

        f2 = self.root.find_path("/etc/nginx/file_dir1/file_dir2/my_file")
        self.assertEqual(f1, f2)

    def test_create_file_dir_with_subs(self):
        self.root.create_file("/q/q/LICENSE")

        f = self.root.find_path("/q/q/LICENSE")
        self.assertEqual("/q/q/LICENSE", f.get_full_path())

    def test_full_path(self):
        f = self.root.create_file("/etc/nginx/file_dir1/file_dir2/my_file")
        self.assertEqual("/etc/nginx/file_dir1/file_dir2/my_file", f.get_full_path())

    def test_full_directory_path_from_file(self):
        f = self.root.create_file("/etc/nginx/another/f1/f2/my_file")
        self.assertEqual("/etc/nginx/another/f1/f2", f.get_full_dir_path())

    def test_full_directory_path_from_dir(self):
        f = self.root.create_dir("/etc/nginx/another/f1/f2/f3")
        self.assertEqual("/etc/nginx/another/f1/f2/f3", f.get_full_dir_path())

    def test_find_file_by_chunk(self):
        f1 = self.root.create_file("/etc/nginx/file")
        f1.chunks['/etc/nginx/file_01'] = "http://localhost:7777"

        f = self.root.find_file_by_chunk("/etc/nginx/file_0")
        self.assertEqual(f1, f)

if __name__ == '__main__':
    unittest.main()
    
    
class NSTests(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.ns = NameServer(dump_on=False)

    def test_put_and_read_file(self):
        r = self.ns.create_file({'path': '/var/some_dir/file', 'size': 1042, 'chunks': {'/var/some_dir/file_01': 'cs-1'}})
        self.assertEqual(Status.ok, r['status'])

        d = self.ns.get_file_info('/var/some_dir/file')
        self.assertEqual(Status.ok, d['status'])
        self.assertEqual(1042, d['size'])
        self.assertEqual(d['chunks']['/var/some_dir/file_01'], 'cs-1')

    def test_list_directory(self):
        self.ns.create_file({'path': '/var/some_dir/file1', 'size': 1042, 'chunks': {'file1_01': 'cs-1'}})
        self.ns.create_file({'path': '/var/some_dir/file2', 'size': 2122, 'chunks': {'file2_01': 'cs-1'}})
        self.ns.create_file({'path': '/var/some_dir/usr/txt', 'size': 2122, 'chunks': {'txt_01': 'cs-1'}})

        r = self.ns.list_directory('/var/some_dir')

        for item, info in r['items'].items():
            info.pop('date', None)

        items = {"file1": {'path': '/var/some_dir/file1', 'type': NodeType.file, 'size': 1042, 'chunks': {'file1_01': 'cs-1'}, 'status': 200},
                 'file2': {'path': '/var/some_dir/file2', 'type': NodeType.file, 'size': 2122, 'chunks': {'file2_01': 'cs-1'}, 'status': 200},
                 'usr': {'path': '/var/some_dir/usr', 'type': NodeType.directory, 'size': 2122, 'chunks': {}, 'status': 200}}

        self.assertEqual(Status.ok, r['status'])
        self.assertDictEqual(items, r['items'])

    def test_root_list(self):
        self.ns.create_file({'path': '/some_dir/file1', 'size': 1042, 'chunks': {'file1_01': 'cs-1'}})
        self.ns.create_file({'path': '/my_dir/file2', 'size': 2122, 'chunks': {'file2_01': 'cs-1'}})
        self.ns.create_file({'path': '/another_file', 'size': 2122, 'chunks': {'txt_01': 'cs-1'}})

        r = self.ns.list_directory('/')

        for item, info in r['items'].items():
            info.pop('date', None)

        items = {'some_dir': NodeType.directory, "my_dir": NodeType.directory, "another_file": NodeType.file}
        items = {"some_dir": {'path': '/some_dir', 'type': NodeType.directory, 'size': 1042,
                           'chunks': {}, 'status': 200},
                 'my_dir': {'path': '/my_dir', 'type': NodeType.directory, 'size': 2122,
                           'chunks': {}, 'status': 200},
                 'another_file': {'path': '/another_file', 'type': NodeType.file, 'size': 2122, 'chunks': {'txt_01': 'cs-1'},
                         'status': 200}}

        self.assertEqual(Status.ok, r['status'])
        self.assertDictEqual(items, r['items'])

    def test_list_empty_root(self):
        r = self.ns.list_directory('/')

        items = {}

        self.assertEqual(Status.ok, r['status'])
        self.assertDictEqual(items, r['items'])

    def test_make_directory(self):
        r = self.ns.make_directory('/my/dir/')
        self.assertEqual(Status.ok, r['status'])

        r = self.ns.make_directory('/my/dir/and/new/dir')
        self.assertEqual(Status.ok, r['status'])

        d = self.ns.get_file_info('/my/dir/and/new/dir')
        self.assertEqual(NodeType.directory, d['type'])

    def test_make_directory_with_error(self):
        self.ns.create_file({'path': '/my/dir/file', 'size': 0, 'chunks': {}})
        r = self.ns.make_directory('/my/dir/file/my_dir')
        self.assertEqual(Status.error, r['status'])
        r = self.ns.get_file_info('/my/dir/file/my_dir')
        self.assertEqual(Status.not_found, r['status'])

    def test_make_directory_already_exists(self):
        self.ns.make_directory('/my/dir/file')
        r = self.ns.make_directory('/my/dir/file')
        self.assertEqual(Status.already_exists, r['status'])

    def test_size_of(self):
        self.ns.create_file({'path': '/my/file', 'size': 100, 'chunks': {}})
        self.ns.create_file({'path': '/my/file2', 'size': 150, 'chunks': {}})

        self.ns.create_file({'path': '/my/dir/file3', 'size': 200, 'chunks': {}})
        self.ns.create_file({'path': '/my/dir/subdir/file5', 'size': 250, 'chunks': {}})

        r = self.ns.size_of('/my')
        self.assertEqual(Status.ok, r['status'])
        self.assertEqual(100+150+200+250, r['size'])

    def test_size_of_not_found(self):
        self.ns.create_file({'path': '/my/dir/file3', 'size': 200, 'chunks': {}})
        self.ns.create_file({'path': '/my/dir/subdir/file5', 'size': 250, 'chunks': {}})

        r = self.ns.size_of('/my/some/path')
        self.assertEqual(Status.not_found, r['status'])

    def test_create_file_exists(self):
        self.ns.create_file({'path': '/my/dir/file3', 'size': 200, 'chunks': {}})
        r = self.ns.create_file({'path': '/my/dir/file3', 'size': 250, 'chunks': {}})

        self.assertEqual(Status.already_exists, r['status'])

    def test_delete_file(self):
        self.ns.create_file({'path': '/my/dir/file3', 'size': 200, 'chunks': {}})
        self.ns.delete('/my/dir/file3')

        r = self.ns.get_file_info('/my/dir/file3')
        self.assertEqual(Status.not_found, r['status'])

    def test_dump(self):
        self.ns.create_file({'path': '/my/dir/file3', 'size': 200, 'chunks': {}})
        self.ns.create_file({'path': '/my/dir/another', 'size': 200, 'chunks': {}})
        self.ns.create_file({'path': '/my/dir/dir3/file3', 'size': 200, 'chunks': {}})
        self.ns.create_file({'path': '/my/dir/dir5/file3', 'size': 200, 'chunks': {}})
        self.ns._dump()
        self.ns._load_dump()

        r = self.ns.get_file_info('/my/dir/another')
        self.assertEqual(Status.ok, r['status'])

    def test_get_cs(self):
        self.ns.heartbeat("localhost:9999")

        r = self.ns.get_cs('/var/something')
        self.assertEqual('localhost:9999', r['cs'])

    def test_check_status(self):
        self.ns.make_directory('/my/dir/file')
        self.ns.create_file({'path': '/some_dir/file1', 'size': 1042, 'chunks': {'file1_01': 'cs-1'}})

        fs = self.ns.get_file_info('/some_dir/file1')
        ds = self.ns.get_file_info('/my/dir/file')

        self.assertEqual(NodeType.file, fs['type'])
        self.assertEqual(NodeType.directory, ds['type'])

if __name__ == '__main__':
    unittest.main()
    
    
    
@unittest.skip("long test should be run manually")
class LongTests(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.ns = NameServer(dump_on=False)

    def test_get_cs_after_timeout(self):
        self.ns.heartbeat("cs-22", "localhost:9999")
        time.sleep(2)

        r = self.ns.get_cs('/var/something')
        self.assertEquals(Status.not_found, r['status'])


if __name__ == '__main__':
    unittest.main()
