#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /pipeline.py                                                                        #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday November 19th 2022 01:30:09 pm                                             #
# Modified   : Sunday November 27th 2022 04:37:09 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
from dataclasses import dataclass
from abc import ABC, abstractmethod
from typing import Any
import logging

from dependency_injector.wiring import Provide, inject

from recsys.core.services.container import Container
from recsys.core.services.io import IOService

# ------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #


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

    def __init__(self, name: str, description: str = None, **kwargs) -> None:
        self._name = name
        self._description = description
        self._steps = {}
        self._context = None

    @property
    def name(self) -> str:
        return self._name

    @property
    def description(self) -> str:
        return self._description

    @property
    def steps(self) -> list:
        return self._steps

    @property
    def data(self) -> list:
        return self._data

    @property
    def context(self) -> Context:
        return self._context

    @context.setter
    def context(self, context: Context) -> None:
        self._context = context

    def add_step(self, step) -> None:
        """Adds a step to the Pipeline object.

        Args:
            step: (Operator): Operator object to add to the pipeline.

        """
        self._steps[step.name] = step

    @abstractmethod
    def run(self) -> None:
        pass

    def _setup(self) -> None:
        """Executes setup for pipeline."""
        pass

    def _teardown(self) -> None:
        """Completes the pipeline process."""
        pass


# ------------------------------------------------------------------------------------------------ #


class PipelineBuilder(ABC):
    """Base class for Pipeline objects"""

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
    def build_steps(self) -> None:
        pass

    @abstractmethod
    def build_pipeline(self) -> None:
        pass


# ------------------------------------------------------------------------------------------------ #


class PipelineDirector:
    """Pipelne director responsible for executing the steps of the PipelineBuilder in a sequence."""

    @inject
    def __init__(
        self, config: dict, builder: PipelineBuilder, io: IOService = Provide[Container.io]
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
        self._builder.build_steps()
        self._builder.build_pipeline()
