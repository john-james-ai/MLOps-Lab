#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /profile.py                                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday December 1st 2022 05:18:02 am                                              #
# Modified   : Thursday December 1st 2022 10:41:44 am                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
from datetime import datetime
# ------------------------------------------------------------------------------------------------ #

# https://www.thepythoncode.com/article/make-a-network-usage-monitor-in-python
#



class Profile:
    def __init__(self, task_id: int, step_id: int, env: str) -> None:
        self._task_id = task_id
        self._step_id = step_id
        self._env = env

        self._reset()

    def reset(self) -> None:
        self._started = None
        self._stopped = None
        self._duration = None
        self._total_memory = None
        self._used_memory = None
        self._free_memory = None
        self._cpu_usage = None
        self._ram_usage = None
        self._bytes_sent = None
        self._bytes_received = None
        self._upload_speed = None
        self._download_speed = None


    def start(self) -> None:
        self._started = datetime.now()
        self._profile()


    def end(self) -> None:
        self._stopped = datetime.now()
        self._duration = (self._stopped-self._started).total_seconds()

    def profile(self) -> None:
        while self._stopped is None:

    task_id: int
    step_id: int
    env: str
    started: datetime
    stopped: datetime
    duration: int
    total_memory: int
    used_memory: int
    free_memory: int
    cpu_usage: float
    ram_usage: float
    bytes_sent: int
    bytes_received: int
    upload_speed: float
    download_speed: float