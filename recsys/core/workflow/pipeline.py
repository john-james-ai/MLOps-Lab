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
# Modified   : Sunday January 1st 2023 01:54:46 am                                                 #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Pipeline Module"""
from collections import OrderedDict
import logging

from recsys import STATES
from recsys.core.entity.job import Job
from recsys.core.workflow.operators.base import Operator
from recsys.core.repo.uow import UnitOfWork


# ------------------------------------------------------------------------------------------------ #
#                                     PIPELINE CLASS                                               #
# ------------------------------------------------------------------------------------------------ #
class Pipeline:
    """Base class for Pipelines"""
    def __init__(self) -> None:
        self._name = None
        self._description = None
        self._mode = None
        self._uow = None

        self._operations = OrderedDict()

        self._logger = logging.getLogger(
            f"{self.__module__}.{self.__class__.__name__}",
        )

    def __str__(self) -> str:
        return f"Pipeline:\n\tModule: {self.__module__}\n\tClass: {self.__class__.__name__}\n\tName: {self._name}\n\tDescription: {self._description}\n\tMode: {self._mode}"

    def __repr__(self) -> str:
        return f"{self.__module__}, {self.__class__.__name__}, {self._name}, {self._description}, {self._mode}"

    def __len__(self) -> int:
        """Returns the number of operations in the pipeline."""
        return len(self._operations)

    # ------------------------------------------------------------------------------------------------ #
    @property
    def name(self) -> str:
        return self._name

    @name.setter
    def name(self, name: str) -> None:
        self._name = name

    # ------------------------------------------------------------------------------------------------ #
    @property
    def description(self) -> str:
        return self._description

    @description.setter
    def description(self, description: str) -> None:
        self._description = description

    # ------------------------------------------------------------------------------------------------ #
    @property
    def mode(self) -> str:
        return self._mode

    @mode.setter
    def mode(self, mode: str) -> None:
        self._mode = mode

    # ------------------------------------------------------------------------------------------------ #
    @property
    def uow(self) -> str:
        return self._uow

    @uow.setter
    def uow(self, uow: UnitOfWork) -> None:
        self._uow = uow

    # ------------------------------------------------------------------------------------------------ #
    def add_operation(self, operator: Operator) -> None:
        """Adds an operation to the pipeline."""
        self._logger.debug(operator)
        self._operations[operator.name] = operator

    def run(self) -> None:
        """Runs the pipeline"""
        self._setup()

        for operator in self._operations.values():
            operator.uow = self._uow
            operator.execute()

        self._teardown()

    def _setup(self) -> None:
        """Creates the Job and returns it."""
        job = self.as_job()
        job.state = STATES[2]
        job = self._uow.job.add(job)
        self._logger.debug(f"Just added the following job:\n{job}")
        self._uow.current_job = job

    def _teardown(self) -> None:
        """Updates job with final state and persists it."""
        self._uow.current_job.state = STATES[-1]
        self._uow.job.update(self._uow.current_job)
        self._logger.debug(f"Just updated the following job:\n{self._uow.current_job}")

    def as_job(self) -> Job:
        """Returns the job object that represents the Pipeline."""
        return Job(
            name=self._name,
            description=self._description,
            mode=self._mode
        )
