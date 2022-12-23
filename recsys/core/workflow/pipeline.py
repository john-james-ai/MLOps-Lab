#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/workflow/pipeline.py                                                   #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday December 19th 2022 03:34:43 pm                                               #
# Modified   : Monday December 19th 2022 08:04:51 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Pipeline Module"""
from abc import ABC, abstractmethod
import pandas as pd

from .task import Task
from recsys.core.dal.repo import Context


# ------------------------------------------------------------------------------------------------ #
#                                     PIPELINE BASE CLASS                                          #
# ------------------------------------------------------------------------------------------------ #
class Pipeline(ABC):
    """Base class for Pipelines"""
    def __init__(self) -> None:
        self._tasks = {}
        self._data = None

    def __len__(self) -> int:
        return len(self._tasks)

    def add_task(self, task: Task) -> None:
        idx = len(self._tasks) + 1
        self._tasks[idx] = task

    @abstractmethod
    def run(self, context: Context) -> None:
        """Runs the pipeline"""

    def _setup(self) -> None:
        """Executes setup for pipeline."""

    def _teardown(self) -> None:
        """Completes the pipeline process."""

    def _task_as_df(self) -> pd.DataFrame:
        return pd.DataFrame.as_dict(self._tasks, orient="index")


# ------------------------------------------------------------------------------------------------ #
#                                      DATA PIPELINE                                               #
# ------------------------------------------------------------------------------------------------ #
class DataPipeline(Pipeline):
    def __init__(self) -> None:
        super().__init__()

    def run(self, context: Context) -> None:
        """Runs the pipeline"""
        self._setup()
        data = self._data

        for _, task in self._tasks.items():
            result = task.run(data, context)
            data = result or data

        self._teardown()
