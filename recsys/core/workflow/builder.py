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
# Modified   : Saturday January 21st 2023 07:53:53 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2023 John James                                                                 #
# ================================================================================================ #
from abc import ABC, abstractmethod
import logging
import importlib

from dependency_injector.wiring import Provide, inject
from dependency_injector import containers

from recsys.container import Recsys
from recsys.core.workflow.dag import DAG, Task
from recsys.core.workflow.operator.base import Operator


# ------------------------------------------------------------------------------------------------ #
class Builder(ABC):
    """Abstract Base Class for DAG Builders"""

    def __init__(self) -> None:
        self._logger = logging.getLogger(
            f"{self.__module__}.{self.__class__.__name__}",
        )

    @property
    @abstractmethod
    def dag(self) -> None:
        """Returns a DAG object."""

    @abstractmethod
    def build_dag(self) -> None:
        """Builds the DAG object"""

    @abstractmethod
    def build_task(self) -> None:
        """Builds the Task object"""

    @abstractmethod
    def build_operator(self) -> None:
        """Builds the Operator object"""


# ------------------------------------------------------------------------------------------------ #
#                                      JOB BUILDER CLASS                                           #
# ------------------------------------------------------------------------------------------------ #


class DAGBuilder(Builder):
    """Constructs a DataSource DAG"""

    @inject
    def __init__(self, factory: containers.DeclarativeContainer = Provide[Recsys.factory]) -> None:
        super().__init__()
        self._factory = factory
        self.reset()

    # ------------------------------------------------------------------------------------------------ #
    @property
    def dag(self) -> DAG:
        return self._dag

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
        self._dag = None

    # ------------------------------------------------------------------------------------------------ #
    def build_dag(self) -> None:
        self._dag = self._factory.dag()(self._config["dag"])
        for config in self._config["tasks"]:
            task = self.build_task(config)
            self._dag.add_task(task)

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
    def builder(self) -> DAGBuilder:
        return self._builder

    @builder.setter
    def builder(self, builder: DAGBuilder) -> None:
        """The Director works with any builder instance.

        Args:
            builder (DAGBuilder): Builder instance.
        """
        self._builder = builder

    def build_dag(self, config: dict) -> DAG:
        self._builder.config = config
        self._builder.build_dag()
        return self._builder.dag
