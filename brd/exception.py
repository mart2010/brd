# -*- coding: utf-8 -*-
__author__ = 'mouellet'




class EltError(Exception):

    def __init__(self, message, cause=None):
        self.message = message
        self.cause = cause

    def __str__(self):
        return self.message

