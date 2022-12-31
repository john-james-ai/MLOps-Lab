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
# Modified   : Friday December 30th 2022 08:08:06 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Job Module"""
from abc import ABC, abstractmethod
import importlib
from types import SimpleNamespace
import logging

from recsys.core.dal.uow import UnitOfWork
from .pipeline import Pipeline
from recsys.core.services.io import IOService


# ------------------------------------------------------------------------------------------------ #
#                                     BUILDER BASE CLASS                                           #
# ------------------------------------------------------------------------------------------------ #
class Builder(ABC):
    """Constructs complex objects"""

    @property
    @abstractmethod
    def pipeline(self) -> Pipeline:
        """Returns a Pipeline object."""

    @abstractmethod
    def build_config(self, config: dict) -> None:
        """Build the configuration namesspace object."""

    @abstractmethod
    def build_pipeline(self) -> None:
        """Builds the job object."""

    @abstractmethod
    def build_operations(self, pipeline: Pipeline) -> None:
        """Builds the pipeline object."""


# ------------------------------------------------------------------------------------------------ #
#                                      JOB BUILDER CLASS                                           #
# ------------------------------------------------------------------------------------------------ #
class PipelineBuilder(Builder):
    """Constructs a Job"""

    def __init__(self) -> None:
        self._uow = None
        self._pipeline = None
        self._logger = logging.getLogger(
            f"{self.__module__}.{self.__class__.__name__}",
        )

    # ------------------------------------------------------------------------------------------------ #
    @property
    def pipeline(self) -> Pipeline:
        return self._pipeline

    @pipeline.setter
    def pipeline(self, pipeline: Pipeline) -> None:
        self._pipeline = pipeline

    # ------------------------------------------------------------------------------------------------ #
    @property
    def uow(self) -> str:
        return self._uow

    @uow.setter
    def uow(self, uow: str) -> None:
        self._uow = uow

    def reset(self) -> Pipeline:
        self._pipeline = Pipeline()

    def build_config(self, config: dict) -> None:
        self._config = SimpleNamespace(**config["pipeline"])

    def build_pipeline(self) -> None:
        self._pipeline.name = self._config.name
        self._pipeline.description = self._config.description
        self._pipeline.mode = self._config.mode
        self._pipeline.uow = self._uow

    def build_operations(self) -> None:
        for task in self._config.tasks:
            task = SimpleNamespace(**task)
            module = importlib.import_module(name=task.module)
            operator = getattr(module, task.operator)
            operator = operator(task_params=task.task_params, output_params=task.output_params)
            self._pipeline.add_operation(operator)


# ------------------------------------------------------------------------------------------------ #
#                                     DIRECTOR CLASS                                               #
# ------------------------------------------------------------------------------------------------ #
class Director:
    """The Director is responsible for executing the building steps in a particular sequence. """

    def __init__(self, pipeline: Pipeline = Pipeline(), uow: UnitOfWork = UnitOfWork(), io: IOService = IOService) -> None:
        self._pipeline = pipeline
        self._uow = uow
        self._io = io
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

    def build_pipeline(self, config_filepath: str) -> None:
        config = self._io.read(config_filepath)
        self._builder.uow = self._uow
        self._builder.pipeline = self._pipeline
        self._builder.build_config(config)
        self._builder.build_pipeline()
        self._builder.build_operations()
        return self._builder.pipeline
