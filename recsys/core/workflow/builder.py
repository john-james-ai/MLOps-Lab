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
# Modified   : Wednesday December 21st 2022 02:58:44 pm                                            #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Job Module"""
from abc import ABC, abstractmethod
import importlib
# from types import SimpleNamespace
import logging

from dependency_injector import providers
from dependency_injector.wiring import Provide, inject

from tests.containers import Recsys
from recsys.data.movielens25m.config import MovieLens25M
from .task import Task
from .pipeline import Pipeline
from .base import Process
from .job import Job
from recsys.core.dal.repo import Context
from recsys.core.workflow.operator import Operator


# ------------------------------------------------------------------------------------------------ #
#                                     BUILDER BASE CLASS                                           #
# ------------------------------------------------------------------------------------------------ #
class Builder(ABC):
    """Constructs complex objects"""

    def __init__(self) -> None:
        self._logger = logging.getLogger(
            f"{self.__module__}.{self.__class__.__name__}",
        )

    @property
    @abstractmethod
    def process(self) -> Process:
        """Returns a Process object."""

    @abstractmethod
    def set_config(self, config: dict) -> None:
        """Sets the pipeline configuration."""

    @abstractmethod
    def set_context(self, context: Context) -> None:
        """Sets the repository context."""

    @abstractmethod
    def set_pipeline(self, pipeline: Pipeline) -> None:
        """Sets the Pipeline object."""

    @abstractmethod
    def build_process(self) -> None:
        """Sets the Process object."""

    @abstractmethod
    def build_tasks(self) -> None:
        """Constructs Task objects and adds to process"""

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
        self._context = None
        self._config = None
        self._pipeline = None

    def reset(self) -> None:
        self._job = Job()

    @property
    def process(self) -> Process:
        job = self._job
        self.reset()
        return job

    def set_config(self, config: Provide) -> None:
        self._logger.debug(config)
        self._config = config()

    def set_context(self, context: Context) -> None:
        self._context = context

    def set_pipeline(self, pipeline: Pipeline) -> None:
        self._pipeline = pipeline

    def build_process(self) -> None:
        self._job.name = self._config.name
        self._job.description = self._config.description
        self._job.mode = self._config.mode
        self._job.context = self._context
        self._job.pipeline = self._pipeline
        self._job = self._context.job.add(self._job)

    def build_tasks(self) -> None:
        """Iterates through task and returns a list of task objects."""
        for task_config in self._config.job.tasks:
            operator = self._build_operator(task_config)
            task = Task()
            task.name = task_config.name
            task.description = task_config.description
            task.mode = task_config.mode
            task.stage = task_config.stage
            task.operator = operator
            task.force = task_config.force
            task.job_id = self._job.id

            self._job.pipeline.add_task(task)

    def save(self) -> None:
        """Updates the job in the repository and saves."""
        self._context.job.update(self._job)
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


    def __init__(self, context: Context = Provide[Recsys.context],
                 config: providers.Configuration = Provide[MovieLens25M.config]) -> None:
        self._builder = None
        self._config = config
        self._context = context

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

    @property
    def process(self) -> Builder:
        return self._process

    @process.setter
    def process(self, process: Process) -> None:
        """The Director works with any process instance.

        Args:
            process (Process): Process instance.
        """
        self._process = process

    @property
    def pipeline(self) -> Pipeline:
        """Pipeline instance."""
        return self._pipeline

    @pipeline.setter
    def pipeline(self, pipeline: Pipeline) -> None:
        """An instance of the Pipeline the job will execute.

        Args:
            pipeline (Pipeline): Pipeline instance.
        """
        self._pipeline = pipeline

    def build_job(self) -> None:
        self._builder.set_config(self._config)  # Class type from container.
        self._builder.set_context(self._context)  # Class type from container.
        self._builder.set_pipeline(self._pipeline)  # Instance
        self._builder.build_process()
        self._builder.build_tasks()
        self._builder.save()
