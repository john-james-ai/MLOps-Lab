#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/workflow/base.py                                                       #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday December 5th 2022 10:24:47 pm                                                #
# Modified   : Friday December 9th 2022 06:49:10 pm                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
from dataclasses import dataclass
from abc import ABC, abstractmethod
from typing import Any
import logging

from dependency_injector.wiring import Provide, inject

from recsys.container import CoreContainer
from recsys.core.services.io import IOService


@dataclass
class Context:
    """Context objects required throughout the pipeline
    Context objects are created during the pipeline build process and are passed as parameters
    to the operator execute methods. They provide pipeline context as well as io operations
    required by the operators.
    Args:
        name (str): Name of the pipeline
        description (str): Optional description for pipeline
        io (Any): Object responsible for persistence and io.
    """

    name: str = None
    description: str = None
    io: Any = None


# ------------------------------------------------------------------------------------------------ #
class Pipeline(ABC):
    """Base class for Pipelines
    Args:
        name (str): Human readable name for the pipeline run.
        description (str): Optional.
    """

    def __init__(
        self, name: str, datasource: str, workspace: str, description: str = None, **kwargs
    ) -> None:
        self._id = None
        self._name = name
        self. _datasource = datasource
        self._workspace = workspace
        self._description = description
        self._logger = logging.getLogger(
            f"{__name__}.{self.__class__.__name__}",
        )
        self._tasks = {}
        self._context = None
        self._profile_id = None
        super().__init__()

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

    @property
    def datasource(self) -> str:
        return self. _datasource

    @property
    def workspace(self) -> str:
        return self._workspace

    @property
    def tasks(self) -> list:
        return self._tasks

    @tasks.setter
    def tasks(self, tasks: dict) -> None:
        self._tasks = tasks

    @property
    def profile_id(self) -> int:
        return self._profile_id

    @profile_id.setter
    def profile_id(self, profile_id: dict) -> None:
        self._profile_id = profile_id

    @property
    def context(self) -> Context:
        return self._context

    @context.setter
    def context(self, context: Context) -> None:
        self._context = context

    def add_task(self, task) -> None:
        """Adds a task to the Pipeline object.
        Args:
            task: (Operator): Operator object to add to the pipeline.
        """
        self._tasks[task.name] = task

    @abstractmethod
    def run(self) -> None:
        pass

    def _setup(self) -> None:
        """Executes setup for pipeline."""

    def _teardown(self) -> None:
        """Completes the pipeline process."""


# ------------------------------------------------------------------------------------------------ #


class PipelineBuilder(ABC):
    """Base class for Pipeline objects"""

    def __init__(self) -> None:
        self._logger = logging.getLogger(
            f"{__name__}.{self.__class__.__name__}",
        )

    def reset(self) -> None:
        self._pipeline = None

    @property
    def pipeline(self) -> Pipeline:
        return self._pipeline

    @abstractmethod
    def build_config(self, config: dict) -> None:
        pass

    @abstractmethod
    def build_context(self, io: IOService) -> None:
        pass

    @abstractmethod
    def build_tasks(self) -> None:
        pass

    @abstractmethod
    def build_pipeline(self) -> None:
        pass


# ------------------------------------------------------------------------------------------------ #


class PipelineDirector:
    """Pipelne director responsible for executing the tasks of the PipelineBuilder in a sequence."""

    @inject
    def __init__(
        self, config: dict, builder: PipelineBuilder, io: IOService = Provide[CoreContainer.io]
    ) -> None:
        self._config = config
        self._builder = builder
        self._io = io

    @property
    def builder(self) -> PipelineBuilder:
        return self._builder

    def build_pipeline(self) -> None:
        """Constructs the  Pipeline"""
        self._builder.build_config(config=self._config)
        self._builder.build_context(io=self._io)
        self._builder.build_tasks()
        self._builder.build_pipeline()
