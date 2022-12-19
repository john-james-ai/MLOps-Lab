#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/entity/job.py                                                          #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday December 5th 2022 10:24:47 pm                                                #
# Modified   : Sunday December 18th 2022 06:52:05 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Job Module."""
from datetime import datetime
from tqdm import tqdm
from typing import List

from recsys.core.entity.base import Entity
from recsys.core.entity.task import Task
from recsys.core.dal.dto import JobDTO


# ------------------------------------------------------------------------------------------------ #
#                                           JOB                                                    #
# ------------------------------------------------------------------------------------------------ #
class Job(Entity):
    """Base class for Jobs
    Args:
        name (str): Human readable name for the job run.
        mode (str): Workspace in which the job runs.
        description (str): Optional.
    """

    def __init__(self, name: str, mode: str, description: str = None, **kwargs) -> None:
        super().__init__(name=name, description=description)
        self._mode = mode

        self._id = None
        self._data = None
        self._started = None
        self._ended = None
        self._duration = None
        self._n_tasks = 0
        self._n_tasks_completed = 0
        self._pct_tasks_completed = 0.0

        self._tasks = []

    @property
    def mode(self) -> str:
        return self._mode

    @property
    def started(self) -> str:
        return self._started

    @property
    def ended(self) -> str:
        return self._ended

    @property
    def duration(self) -> str:
        return self._duration

    @property
    def n_tasks(self) -> str:
        return self._n_tasks

    @property
    def n_tasks_completed(self) -> str:
        return self._n_tasks_completed

    @property
    def tasks(self) -> List[Task]:
        return self._tasks

    @tasks.setter
    def tasks(self, tasks: List[Task]) -> None:
        self._tasks = tasks

    def add_task(self, task) -> None:
        """Adds a task to the Job object.
        Args:
            task: (Operator): Operator object to add to the job.
        """
        self._tasks[task.id] = task
        self._n_tasks += 1

    def run(self) -> None:
        """Iterates through tasks"""
        self._setup()
        for id, task in tqdm(self._tasks.items()):
            result = task.run(self._data)
            self._data = result if result is not None else self._data
            self._n_tasks_completed += 1
        self._teardown()

    def as_dto(self) -> JobDTO:
        return JobDTO(
            id=self._id,
            name=self._name,
            description=self._description,
            mode=self._mode,
            stage=self._stage,
            n_tasks=self._n_tasks,
            n_tasks_completed=self._n_tasks_completed,
            pct_tasks_completed=self._pct_tasks_completed,
            started=self._started,
            ended=self._ended,
            duration=self._duration,
            created=self._created,
            modified=self._modified,
        )

    def _from_dto(self, dto: JobDTO) -> None:
        super().__init__(name=dto.name, description=dto.description)
        self._id = dto.id
        self._name = dto.name
        self._description = dto.description
        self._mode = dto.mode
        self._stage = dto.stage
        self._n_tasks = dto.n_tasks
        self._n_tasks_completed = dto.n_tasks_completed
        self._pct_tasks_completed = dto.pct_tasks_completed
        self._started = dto.started
        self._ended = dto.ended
        self._duration = dto.duration
        self._created = dto.created
        self._modified = dto.modified
        self._validate()

    def _setup(self) -> None:  # pragma: no cover
        """Executes setup for job."""
        self._started = datetime.now()

    def _teardown(self) -> None:  # pragma: no cover
        """Completes the job process."""
        self._ended = datetime.now()
        self._duration = (self._ended - self._started).total_seconds()
        try:
            self._pct_tasks_completed = (self._n_tasks_completed / self._n_tasks) * 100
        except ZeroDivisionError:
            self._pct_tasks_completed = 0.0
