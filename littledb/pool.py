# Created by lujin at 15/4/2017


class Pool(object):
    def __init__(self, creator, init_size, max_size=20, **kwargs):
        self._creator = creator
        self._max_size = max_size
        if init_size > max_size:
            self._init_size = max_size
