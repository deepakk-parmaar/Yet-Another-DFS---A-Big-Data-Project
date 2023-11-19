#!/usr/bin/env python3.11
import click
from client.client import Client
from utils.enums import Status, NodeType
import getpass
import datetime
import os
import shutil
import difflib

# CLI
@click.group(invoke_without_command=False)
@click.pass_context
def cli(ctx):
    pass

@cli.command()
@click.argument('path', default="/")
def cd(path):
    '''Change the current directory'''
    try:
        os.chdir(path)
        print(f"Changed directory to {os.getcwd()}")
    except FileNotFoundError:
        print(f"Directory '{path}' not found.")
    except Exception as e:
        print(f"Error changing directory: {e}")

@cli.command()
@click.argument('path', default="/")
def ls(path):
    """List directory contents"""
    cl = Client()
    dir_ls = cl.list_dir(path)
    stat = dir_ls['status']
    if stat == Status.ok:
        for item, info in dir_ls['items'].items():
            fr = "-rw-r--r--"
            if info['type'] == NodeType.directory:
                fr = "drwxr-xr-x"
            date = datetime.datetime.fromtimestamp(info['date'])
            date_format = '%b %d %H:%M' if date.year == datetime.datetime.today().year else '%b %d %Y'
            print('%.11s   %.10s   %6sB   %.15s    %s' % (fr, getpass.getuser(), info['size'],                                datetime.datetime.fromtimestamp(info['date']).strftime(date_format), item))
    else:
        print(Status.description(stat))

@click.command()
def pwd():
    """Print the current working directory"""
    current_directory = os.getcwd()
    print(current_directory)

@cli.command()
@click.argument('path')
def mkdir(path):
    """Create a directory"""
    cl = Client()
    res = cl.create_dir(path)
    stat = res['status']
    if stat != Status.ok:
        print(Status.description(stat))

@cli.command()
@click.argument('source')
@click.argument('destination')
def cp(source, destination):
    """Copy files or directories"""
    try:
        shutil.copy(source, destination)
        print(f"Copy successful: {source} to {destination}")
    except FileNotFoundError:
        print(f"Error: File not found: {source}")
    except PermissionError:
        print(f"Error: Permission denied. Check permissions for {source}")
    except Exception as e:
        print(f"Error: {e}")

@cli.command()
@click.argument('local_path')
@click.argument('remote_path', default="/")
def upload(local_path, remote_path):
    """Create a file"""
    cl = Client()

    if os.path.isdir(local_path):
        print("You can't upload directory as a file")
    else:
        res = cl.create_file(local_path, remote_path)
        stat = res['status']
        if stat != Status.ok:
            print(Status.description(stat))

@cli.command()
@click.argument('path')
def rm(path):
    """Delete a file or directory"""
    cl = Client()
    res = cl.delete(path)
    stat = res['status']
    if stat != Status.ok:
        print(Status.description(stat))

@cli.command()
@click.argument('source')
@click.argument('destination')
def mv(source, destination):
    """Move or rename files or directories"""
    try:
        shutil.move(source, destination)
        print(f"Move successful: {source} to {destination}")
    except FileNotFoundError:
        print(f"Error: File not found: {source}")
    except PermissionError:
        print(f"Error: Permission denied. Check permissions for {source}")
    except Exception as e:
        print(f"Error: {e}")

@cli.command()
@click.argument('path')
def status(path):
    """Check if path refers to file or directory"""
    cl = Client()
    res = cl.path_status(path)
    print('%s is a %s' % (path, NodeType.description(res['type'])))

@cli.command()
def date():
    """Print the current date and time"""
    current_date_time = datetime.datetime.now()
    print(current_date_time)

@cli.command()
@click.argument('path_from')
@click.argument('path_to')
def download(path_from, path_to):
    """Download a file"""
    cl = Client()
    res = cl.download_file(path_from, path_to)
    # print(res)
    stat = res['status']
    if stat != Status.ok:
        print(Status.description(stat))

@cli.command()
@click.argument('path', default='.')
def du(path):
    """Display file and directory space usage"""
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(path):
        for filename in filenames:
            filepath = os.path.join(dirpath, filename)
            total_size += os.path.getsize(filepath)
    print(f"Total size of '{path}': {total_size} bytes")

@cli.command()
@click.argument('file1')
@click.argument('file2')
def diff(file1, file2):
    """Display the differences between two files"""
    with open(file1, 'r') as f1, open(file2, 'r') as f2:
        diff_lines = difflib.unified_diff(f1.readlines(), f2.readlines())
        for line in diff_lines:
            print(line, end='')
            
@cli.command()
@click.argument('file_path')
def wc(file_path):
    """Count lines, words, and characters in a file"""
    with open(file_path, 'r') as f:
        content = f.read()
        line_count = len(content.splitlines())
        word_count = len(content.split())
        char_count = len(content)
        print(f"Lines: {line_count}, Words: {word_count}, Characters: {char_count}")


@cli.command()
@click.argument('path')
def cat(path):
    """Print a file"""
    cl = Client()
    res = cl.get_file_content(path)
    print(res)

if __name__ == '__main__':
    cli()
