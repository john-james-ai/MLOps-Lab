#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/factory/job.py                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday January 14th 2023 07:43:33 pm                                              #
# Modified   : Saturday January 14th 2023 09:54:44 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2023 John James                                                                 #
# ================================================================================================ #
from recsys.core.factory.base import Factory
from recsys.core.entity.job import Job, Task


# ------------------------------------------------------------------------------------------------ #
#                                      JOB FACTORY                                                 #
# ------------------------------------------------------------------------------------------------ #
class JobFactory(Factory):
    def __init__(self) -> None:
        super().__init__()
        self._instance = None

    def __call__(self, config: dict) -> Job:
        if not self._instance:
            self._instance = self._build_entity(config)
        return self._instance

    def _build_entity(self, config: dict) -> Job:
        job = Job(**config)
        return job


# ------------------------------------------------------------------------------------------------ #
#                                   TASK FACTORY                                                   #
# ------------------------------------------------------------------------------------------------ #
class TaskFactory(Factory):
    def __init__(self) -> None:
        super().__init__()
        self._instance = None

    def __call__(self, config: dict) -> Task:
        if not self._instance:
            self._instance = self._build_entity(config)
        return self._instance

    def _build_entity(self, config: dict) -> Task:
        return Task(**config)
