#!/usr/bin/env python3.11
class NodeType:
    @staticmethod
    def description(stat):
    	node = ''
        if stat == 2:
            node = 'directory'
        elif stat == 1:
            node = 'file'
        return node

class Status:
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
