#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/workflow/job.py                                                        #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday December 5th 2022 10:24:47 pm                                                #
# Modified   : Wednesday December 28th 2022 06:28:46 am                                            #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Job Module."""
from collections import OrderedDict

from recsys import STATES
from recsys.core.dal.dto import JobDTO
from recsys.core.workflow.task import Task
from .base import Process


# ------------------------------------------------------------------------------------------------ #
#                                           JOB                                                    #
# ------------------------------------------------------------------------------------------------ #
class Job(Process):
    """Base class for Jobs
    Args:
        name (str): Human readable name for the job run.
        description (str): Optional.
        mode (str): Mode in which the job runs.

    """

    def __init__(self, name: str, description: str = None, mode: str = None) -> None:
        super().__init__(name=name, description=description, mode=mode)

        self._tasks = OrderedDict()

    def add_task(self, task: Task) -> None:
        self._tasks[task.name] = task

    def run(self) -> None:
        """Runs the Job"""
        self._setup()
        for task in self._tasks.values():
            dataset = self._uow.context.dataset.read_by_name(name=task.input_spec.name)
            dataset = task.run(data=dataset)
            self._uow.context.dataset.create(dataset)

        self._pipeline.run(uow=self._uow)
        self._teardown()

    def as_dto(self) -> JobDTO:
        return JobDTO(
            id=self._id,
            name=self._name,
            description=self._description,
            mode=self._mode,
            started=self._started,
            ended=self._ended,
            duration=self._duration,
            state=self._state,
            created=self._created,
            modified=self._modified,
        )

    def _from_dto(self, dto: JobDTO) -> None:
        super().__init__(name=dto.name, description=dto.description)
        self._id = dto.id
        self._mode = dto.mode
        self._started = dto.started
        self._ended = dto.ended
        self._duration = dto.duration
        self._state = dto.state
        self._created = dto.created
        self._modified = dto.modified

        self._pipeline = None

        self._validate()

    def _validate(self) -> None:
        super()._validate()
        if self._state not in STATES:
            msg = f"Invalid 'state'. Valid values are {STATES}."
            self._state = STATES[3]
            self._logger.error(msg)
            raise ValueError(msg)
