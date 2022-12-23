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
# Modified   : Tuesday December 20th 2022 06:30:59 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Job Module."""
from datetime import datetime

from recsys.core.dal.dto import JobDTO
from .base import Process, STATES
from .pipeline import Pipeline
from recsys.core.dal.repo import Context


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

    def __init__(self) -> None:
        super().__init__()
        self._pipeline = None
        self._context = None

    @property
    def context(self) -> Context:
        return self._context

    @context.setter
    def context(self, context: Context) -> None:
        self._context = context

    @property
    def pipeline(self) -> Pipeline:
        return self._pipeline

    @pipeline.setter
    def pipeline(self, pipeline: Pipeline) -> None:
        if self._pipeline is None:
            self._pipeline = pipeline
            self._modified = datetime.now()
        else:
            msg = "Job doesn't support 'pipeline' re-assignment."
            self._logger.error(msg)
            raise TypeError(msg)

    def run(self) -> None:
        """Runs the Job"""
        self._setup()
        self._pipeline.run(context=self._context)
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
