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
# Created    : Saturday January 21st 2023 03:07:56 am                                              #
# Modified   : Saturday January 21st 2023 03:09:25 am                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2023 John James                                                                 #
# ================================================================================================ #
from abc import ABC, abstractmethod
import logging
import importlib

from dependency_injector.wiring import Provide, inject
from dependency_injector import containers

from recsys.containers import Recsys
from recsys.core.workflow.process import Job, Task
from recsys.core.workflow.operator.base import Operator


# ------------------------------------------------------------------------------------------------ #
class Builder(ABC):
    """Abstract Base Class for Job Builders"""

    def __init__(self) -> None:
        self._logger = logging.getLogger(
            f"{self.__module__}.{self.__class__.__name__}",
        )

    @property
    @abstractmethod
    def job(self) -> None:
        """Returns a Job object."""

    @abstractmethod
    def build_job(self) -> None:
        """Builds the Job object"""

    @abstractmethod
    def build_task(self) -> None:
        """Builds the Task object"""

    @abstractmethod
    def build_operator(self) -> None:
        """Builds the Operator object"""


# ------------------------------------------------------------------------------------------------ #
#                                      JOB BUILDER CLASS                                           #
# ------------------------------------------------------------------------------------------------ #


class JobBuilder(Builder):
    """Constructs a DataSource Job"""

    @inject
    def __init__(self, factory: containers.DeclarativeContainer = Provide[Recsys.factory]) -> None:
        super().__init__()
        self._factory = factory
        self.reset()

    # ------------------------------------------------------------------------------------------------ #
    @property
    def job(self) -> Job:
        return self._job

    # ------------------------------------------------------------------------------------------------ #
    @property
    def config(self) -> dict:
        return self._config

    # ------------------------------------------------------------------------------------------------ #
    @config.setter
    def config(self, config: dict) -> None:
        self._config = config

    # ------------------------------------------------------------------------------------------------ #
    def reset(self) -> None:
        self._job = None

    # ------------------------------------------------------------------------------------------------ #
    def build_job(self) -> None:
        self._job = self._factory.job()(self._config["job"])
        for config in self._config["tasks"]:
            task = self.build_task(config)
            self._job.add_task(task)

    # ------------------------------------------------------------------------------------------------ #
    def build_task(self, config: dict) -> Task:
        task = self._factory.task()(config["task"])
        operator = self.build_operator(config["operator"])
        task.operator = operator
        return task

    # ------------------------------------------------------------------------------------------------ #
    def build_operator(self, config) -> Operator:
        module = importlib.import_module(name=config["module"])
        operator = getattr(module, config["name"])
        return operator(config["params"])


# ------------------------------------------------------------------------------------------------ #
#                                     DIRECTOR CLASS                                               #
# ------------------------------------------------------------------------------------------------ #
class Director:
    """The Director is responsible for executing the building steps in a particular sequence."""

    def __init__(self) -> None:
        self._builder = None

    @property
    def builder(self) -> JobBuilder:
        return self._builder

    @builder.setter
    def builder(self, builder: JobBuilder) -> None:
        """The Director works with any builder instance.

        Args:
            builder (JobBuilder): Builder instance.
        """
        self._builder = builder

    def build_job(self, config: dict) -> Job:
        self._builder.config = config
        self._builder.build_job()
        return self._builder.job
