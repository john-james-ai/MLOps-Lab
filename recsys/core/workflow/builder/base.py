#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/workflow/builder/base.py                                               #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday December 19th 2022 03:34:43 pm                                               #
# Modified   : Saturday January 14th 2023 03:02:11 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Job Module"""
from abc import ABC, abstractmethod
import importlib
import logging

from recsys.core.entity.base import Entity
from recsys.core.entity.job import Job, Task
from recsys.core.workflow.operator.base import Operator
from recsys.core.services.io import IOService
from recsys.core.entity.datasource import DataSource, DataSourceURL


# ------------------------------------------------------------------------------------------------ #
#                                        BUILDER                                                   #
# ------------------------------------------------------------------------------------------------ #
class Builder(ABC):
    """Builder abstract base class for Entity Builder subclasses."""

    def __init__(self, io: IOService = IOService()) -> None:
        self._instance = None
        self._io = io
        self._logger = logging.getLogger(
            f"{self.__module__}.{self.__class__.__name__}",
        )

    def __call__(self, config) -> Entity:
        """Returns an instance of the requested entity"""


# ------------------------------------------------------------------------------------------------ #
#                                         FACTORY                                                  #
# ------------------------------------------------------------------------------------------------ #
class Factory(ABC):
    """Abstract Base Class for Generic Object Factory containing Entity Builders."""

    def __init__(self) -> None:
        self._logger = logging.getLogger(
            f"{self.__module__}.{self.__class__.__name__}",
        )

    @abstractmethod
    def register_builder(self, key, builder):
        """Registers builder and its key for the entity"""

    @abstractmethod
    def create(self, key, **config):
        """Creates the object"""


# ------------------------------------------------------------------------------------------------ #
#                                     ENTITY FACTORY                                               #
# ------------------------------------------------------------------------------------------------ #
class EntityFactory(Factory):
    """Generic Entity Factory"""

    def __init__(self):
        self._builders = {}

    def register_builder(self, key, builder):
        self._builders[key] = builder

    def create(self, key, config):
        try:
            builder = self._builders.get(key)
            return builder(config)
        except KeyError:
            msg = f"No builder for {key} exists."
            self._logger.error(msg)
            raise


# ------------------------------------------------------------------------------------------------ #
#                                      JOB BUILDER CLASS                                           #
# ------------------------------------------------------------------------------------------------ #
class DataSourceJobBuilder(JobBuilder):
    """Constructs a DataSource Job"""

    def __init__(self, config_filepath: str, io: IOService = IOService()) -> None:
        self._config = io.read(config_filepath)["datasources"]

        self._job = None
        self._logger = logging.getLogger(
            f"{self.__module__}.{self.__class__.__name__}",
        )

    # ------------------------------------------------------------------------------------------------ #
    @property
    def job(self) -> Job:
        return self._job

    # ------------------------------------------------------------------------------------------------ #
    def reset(self) -> None:
        self._job = None

    def build_job(self) -> None:
        name = self._config["name"]
        description = self._config["description"]
        self._job = Job(name=name, description=description)
        for task_config in self._config["tasks"]:
            task = self.build_task(task_config)
            self._job.add_task(task)

    def build_task(self, task_config) -> Task:
        operator = self.build_operator(task_config["operator"])
        task = Task(name=task_config["name"], description=task_config["name"], operator=operator)
        return task

    def build_operator(self, operator_config) -> Operator:
        datasource = self.build_datasource(operator_config["params"]["datasource"])
        module = importlib.import_module(name=operator_config["module"])
        operator = getattr(module, operator_config["classname"])
        return operator(
            name=operator_config["name"],
            description=operator_config["description"],
            datasource=datasource,
        )

    def build_datasource(self, datasource_config) -> DataSource:
        datasource = DataSource(
            name=datasource_config["name"],
            description=datasource_config["description"],
            website=datasource_config["website"],
        )
        for url in datasource_config["urls"]:
            url = self.build_datasource_url(url=url, datasource=datasource)
            datasource.add_url(url)
        return datasource

    def build_datasource_url(self, url: dict, datasource: DataSource) -> DataSourceURL:
        return DataSourceURL(
            name=url["name"],
            description=url["description"],
            url=url["url"],
            datasource=datasource,
        )


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

    def build_datasource_job(self) -> Job:
        self._builder.build_job()
        return self._builder.job
