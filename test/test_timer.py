import unittest
import time

from common.my_timer import RecurTimer, DecayTimer


class TestRecurTimer(unittest.TestCase):
    def check_func(self):
        self.tries -= 1
        curr_time = time.time()
        interval = curr_time - self.last_time
        # print('after interval', interval, self.tries)
        assert(interval > self.interval)
        if self.tries == 0:
            self.timer.cancel()
            return
        self.last_time = curr_time

    def test_long_interval(self):
        self.tries = 10
        self.interval = 1/10
        self.timer = RecurTimer(self.interval, self.check_func)
        self.last_time = time.time()
        self.timer.start()

    def test_short_interval(self):
        self.tries = 10
        self.interval = 1/1000
        self.timer = RecurTimer(self.interval, self.check_func)
        self.last_time = time.time()
        self.timer.start()


class TestDecayTimer(unittest.TestCase):
    def check_func(self, barrier):
        curr_time = time.time()
        interval = curr_time - self.last_time
        # print('after interval', interval, self.tries)
        self.tries += 1
        assert(interval > self.interval)
        assert(self.tries <= barrier)
        if self.tries == 0:
            self.timer.cancel()
            return
        self.last_time = curr_time
        self.interval *= 2

    def test_long_interval(self):
        self.tries = 0
        self.interval = 1/10
        self.timer = DecayTimer(self.interval, self.check_func, [4])
        self.last_time = time.time()
        self.timer.start()

    def test_short_interval(self):
        self.tries = 0
        self.interval = 1/1000
        self.timer = DecayTimer(self.interval, self.check_func, [4])
        self.last_time = time.time()
        self.timer.start()


if __name__ == '__main__':
    unittest.main()
