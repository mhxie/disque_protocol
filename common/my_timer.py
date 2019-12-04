#!/usr/bin/env python3

from threading import Timer, Thread, Event

RETRY_TIMES = 4


class RecurTimer(object):
    def __init__(self, interval, func, arg_list=[]):
        self.interval = interval
        self.func = func
        self.arg_list = arg_list
        self.timer = Timer(interval, self.handle_func)

    def handle_func(self):
        if self.cancelled:
            return
        self.func(*self.arg_list)
        self.timer = Timer(self.interval, self.handle_func)
        self.timer.start()

    def start(self):
        self.cancelled = False
        self.timer.start()

    def cancel(self):
        self.cancelled = True
        self.timer.cancel()


class DecayTimer(RecurTimer):
    def __init__(self, int, func, arg_list=[]):
        super().__init__(int, func, arg_list)
        self.retry = RETRY_TIMES

    def handle_func(self):
        # print('retry', self.retry, 'times left')
        if self.retry == 0:
            self.timer.cancel()
            return
        self.interval *= 2
        self.retry -= 1
        self.func(*self.arg_list)
        self.timer = Timer(self.interval, self.handle_func)
        self.timer.start()
