#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/workflow/builder.py                                                    #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday December 19th 2022 03:34:43 pm                                               #
# Modified   : Wednesday December 28th 2022 06:32:06 am                                            #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Job Module"""
from abc import ABC, abstractmethod
import importlib
from types import SimpleNamespace
import logging

from recsys.core.repo.uow import UnitOfWork
from .task import Task
from .pipeline import Pipeline
from .job import Job
from recsys.core.services.io import IOService
from recsys.core.repo.base import Context
from recsys.core.workflow.operator import Operator


# ------------------------------------------------------------------------------------------------ #
#                                     BUILDER BASE CLASS                                           #
# ------------------------------------------------------------------------------------------------ #
class Builder(ABC):
    """Constructs complex objects"""

    @inject
    def __init__(self, uow: UnitOfWork()) -> None:
        self._uow = uow
        self._logger = logging.getLogger(
            f"{self.__module__}.{self.__class__.__name__}",
        )

    @property
    @abstractmethod
    def job(self) -> Job:
        """Returns a Job object."""

    @abstractmethod
    def build_config(self, config: dict) -> None:
        """Build the configuration namesspace object."""

    @abstractmethod
    def build_job(self, job_config: SimpleNamespace) -> None:
        """Builds the job object."""

    @abstractmethod
    def build_pipeline(self, pipeline: Pipeline) -> None:
        """Builds the pipeline object."""

    @abstractmethod
    def build_tasks(self, tasks_config: SimpleNamespace) -> None:
        """Constructs Task objects and adds to the pipeline"""

    @abstractmethod
    def save(self) -> None:
        """Saves the process in the repository."""


# ------------------------------------------------------------------------------------------------ #
#                                      JOB BUILDER CLASS                                           #
# ------------------------------------------------------------------------------------------------ #
class JobBuilder(Builder):
    """Constructs a Job"""

    def __init__(self) -> None:
        super().__init__()
        self.reset()
        self._pipeline = None

    def reset(self) -> None:
        self._job = Job()

    @property
    def job(self) -> Job:
        job = self._job
        self.reset()
        return job

    def build_config(self, config: dict) -> None:
        self._config = SimpleNamespace(**config)

    def build_job(self) -> None:
        self._job.name = self._config.name
        self._job.description = self._config.description

        self._job = self._context.job.add(self._job)

    def build_pipeline(self) -> None:
        module = importlib.import_module(name=self._config.pipeline.module)
        pipeline = getattr(module, self._config.pipeline.name)
        self._job.pipeline = pipeline()
        self._context.job.update(self._job)

    def build_tasks(self) -> None:
        """Iterates through task and returns a list of task objects."""
        for task_config in self._config.job.pipeline.tasks:
            operator = self._build_operator(task_config)
            task = Task()
            task.name = task_config.name
            task.description = task_config.description
            task.mode = task_config.mode
            task.stage = task_config.stage
            task.operator = operator
            task.force = task_config.force
            task.job_id = self._job.id
            self._context.task.add(task)

            self._job.pipeline.add_task(task)

        self._context.job.update(self._job)

    def save(self) -> None:
        """Saves context."""
        self._context.save()

    def _build_operator(self, config) -> Operator:
        """Contructs the Operation object.

        Args:
            config (SimpleNamespace): Job configuration.
        """
        module = importlib.import_module(name=config.module)
        operator = getattr(module, config.operator)

        return operator(**vars(config.params))


# ------------------------------------------------------------------------------------------------ #
#                                     DIRECTOR CLASS                                               #
# ------------------------------------------------------------------------------------------------ #
class Director:
    """The Director is responsible for executing the building steps in a particular sequence. """

    def __init__(self, io: IOService = IOService) -> None:
        self._builder = None
        self._config = None

    @property
    def builder(self) -> Builder:
        return self._builder

    @builder.setter
    def builder(self, builder: Builder) -> None:
        """The Director works with any builder instance.

        Args:
            builder (Builder): Builder instance.
        """
        self._builder = builder

    def build_job(self, config_filepath: str) -> None:
        config = IOService.read(config_filepath)
        self._builder.build_config(config["job"])
        self._builder.build_job()
        self._builder.build_pipeline()
        self._builder.build_tasks()
        self._builder.save()
