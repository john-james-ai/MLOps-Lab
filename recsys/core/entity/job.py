#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/services/job.py                                                        #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday December 5th 2022 10:24:47 pm                                                #
# Modified   : Tuesday December 13th 2022 05:13:17 am                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Job Module."""
import importlib
from dataclasses import dataclass
from abc import ABC, abstractmethod
import logging
from tqdm import tqdm
from dependency_injector import containers

from recsys.core.database.sqlite import SQLiteDatabase, Database
from recsys.core.services.base import Service
from recsys.core.services.context import Context

# ------------------------------------------------------------------------------------------------ #
#                                           JOB                                                    #
# ------------------------------------------------------------------------------------------------ #
class Job(Service):
    """Base class for Jobs
    Args:
        name (str): Human readable name for the job run.
        description (str): Optional.
    """

    def __init__(self, name: str, description: str = None, **kwargs) -> None:
        super().__init__()
        self._id = None
        self._name = name
        self._description = description

        self._tasks = {}

    @property
    def id(self) -> int:
        return self._id

    @id.setter
    def id(self, id: int) -> None:
        if self._id is None:
            self._id = id
        else:
            msg = "The id member doesn't support reassignment."
            self._logger.error(msg)
            raise TypeError(msg)

    @property
    def name(self) -> str:
        return self._name

    @property
    def description(self) -> str:
        return self._description

    def add_task(self, task) -> None:
        """Adds a task to the Job object.
        Args:
            task: (Operator): Operator object to add to the job.
        """
        self._tasks[task.name] = task

    @abstractmethod
    def run(self) -> None:
        pass

    def _setup(self) -> None:
        """Executes setup for job."""

    def _teardown(self) -> None:
        """Completes the job process."""


# ------------------------------------------------------------------------------------------------ #
#                                        JOB BUILDER                                               #
# ------------------------------------------------------------------------------------------------ #
class JobBuilder(Service):
    """Base class for Job objects"""

    def reset(self) -> None:
        self._job = None
        self._config = None

    @property
    def job(self) -> Job:
        return self._job

    @abstractmethod
    def build_config(self, *args, **kwargs) -> None:
        """Extracts the configuration from the container."""

    @abstractmethod
    def build_dependencies(self, *args, **kwargs) -> None:
        """Constructs and wires dependencies"""

    @abstractmethod
    def build_job(self) -> None:
        """Initializes a job instance."""

    @abstractmethod
    def build_tasks(self) -> None:
        """Builds the tasks"""


# ------------------------------------------------------------------------------------------------ #
#                                       OUTSIDE JOB BUILDER
# ------------------------------------------------------------------------------------------------ #
class OutsideJobBuilder(JobBuilder):
    """Constructs an  processing job."""

    def __init__(self, container: containers.Container) -> None:
        super().__init__()
        self.reset()
        self._container = container

    def reset(self) -> None:
        super().reset()
        self._context = None

    def build_config(self) -> None:
        """Extracts the configuration from the  container."""
        self._config = self.container.config

    def build_dependencies(self) -> None:
        """Wires the dependencies container."""
        self._container.init_resources()
        self._container.wire(modules=[self._config.pipeline.module])

    def build_context(self) -> None:
        """Builds the Database Context"""
        self._context = Context(database=self._container.database)

    def build_job(self) -> None:
        """Initializes a Job instance."""
        self._job = OutsideJob(
            name=self._config.pipeline.name,
            description=self._config.pipeline.description
        )
        repo = self._context.repo
        self._job = repo.job.add(self._job)



    def build_tasks(self) -> None:
        """Constructs the tasks and adds to job."""

        for task in self._config.job.tasks:
            try:
                module = importlib.import_module(name=self._config.job.module)
                task = getattr(module, task)

                operator = task(
                    name=task_config["name"],
                    description=task_config["description"],
                    specs=task_config["specs"],
                    input=task_config["input"],
                    output=[output for output in task_config["output"]],
                    force=task_config["force"],
                )

                self._tasks[operator.name] = operator

            except KeyError as e:  # pragma: no cover
                self._logger.error("Configuration File is missing operator configuration data")
                raise (e)

    def build_job(self) -> None:
        self._job = DataJob(
            name=self._config["name"], description=self._config["description"]
        )
        self._job.context = self._context
        for _, task in self._tasks.items():
            self._job.add_task(task)

# ------------------------------------------------------------------------------------------------ #
#                                        JOB DIRECTOR                                              #
# ------------------------------------------------------------------------------------------------ #
class JobDirector:
    """Job director controls the job build process."""

    def __init__(self, config: dict, builder: JobBuilder) -> None:
        self._config = config
        self._builder = builder

    @property
    def builder(self) -> JobBuilder:
        return self._builder

    def build_job(self) -> None:
        """Constructs the  Job"""
        self._builder.build_config(config=self._config)
        self._builder.build_dependencies()
        self._builder.build_job()
        self._builder.build_tasks()


# ------------------------------------------------------------------------------------------------ #
#                                      OUTSIDE JOB                                                 #
# ------------------------------------------------------------------------------------------------ #
class OutsideJob(Job):
    """Job class for jobs that are performed outside the environment wrapper.

    Outside jobs have the context come to them, rather than them going to the context or
    environment.

    Args:
        name (str): Name for job
        description (str): Description for job.
    """

    def __init__(self, name: str, description: str = None, **kwargs) -> None:
        super().__init__(name=name, description=description)
        self._context = None
        self._data = None


    @property
    def data(self) -> Context:
        return self._data

    @data.setter
    def data(self, data: Context) -> None:
        self._data = data

    @property
    def context(self) -> Context:
        return self._context

    @context.setter
    def context(self, context: Context) -> None:
        self._context = context

    def run(self) -> None:
        """Runs the job"""
        self._setup()
        for name, task in tqdm(self._tasks.items()):

            result = task.run(data=self._data, context=self._context)
            self._data = result if result is not None else self._data

        self._teardown()
