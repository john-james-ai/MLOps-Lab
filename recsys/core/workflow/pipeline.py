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
# Modified   : Wednesday November 30th 2022 11:11:28 pm                                            #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
from dataclasses import dataclass
from abc import ABC, abstractmethod
from typing import Any
import logging
import importlib
from copy import deepcopy

from tqdm import tqdm
from dependency_injector.wiring import Provide, inject

from recsys.core.services.container import Container
from recsys.core.services.io import IOService
from recsys.core.dal.repo import Repo

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
        io (IOService): Object responsible for reading and writing files
        repo (Repo): Repository for Dataset objects, Models, etc...
    """

    name: str = None
    description: str = None
    io: IOService = None
    repo: Repo = None


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


# ------------------------------------------------------------------------------------------------ #
#                                     DATA PIPELINE                                                #
# ------------------------------------------------------------------------------------------------ #


class DataPipeline(Pipeline):
    """Pipeline class

    Executes Extract-Transform-Load pipelines

    Args:
        name (str): Human readable name for the pipeline run.
        description (str): Optional.
    """

    def __init__(self, name: str, description: str = None) -> None:
        super().__init__(name=name, description=description)

    def run(self) -> None:
        """Runs the pipeline"""
        self._data = None
        self._setup()
        for name, step in tqdm(self._steps.items()):

            result = step.run(data=self._data, context=self._context)
            self._data = result if result is not None else self._data

        self._teardown()


# ------------------------------------------------------------------------------------------------ #


class DataPipelineBuilder(PipelineBuilder):
    """Constructs an  processing pipeline."""

    def __init__(self) -> None:
        super().__init__()
        self.reset()
        self._config = None
        self._context = None
        self._steps = {}

    @property
    def config(self) -> dict:
        return self._config

    @property
    def context(self) -> Context:
        return self._context

    @property
    def steps(self) -> dict:
        return self._steps

    def build_config(self, config: dict) -> None:
        self._config = config

    def build_context(self, io: IOService) -> None:
        try:
            self._context = Context(
                name=self._config["name"], description=self._config["description"], io=io
            )
        except KeyError as e:  # pragma: no cover
            logger.error(e)
            raise

    def build_steps(self) -> None:
        steps = self._config["steps"]
        for _, step_config in steps.items():
            try:
                module = importlib.import_module(name=step_config["module"])
                step = getattr(module, step_config["operator"])

                operator = step(
                    **step_config["params"],
                )

                self._steps[operator.name] = operator

            except KeyError as e:  # pragma: no cover
                logging.error("Configuration File is missing operator configuration data")
                raise (e)

    def build_pipeline(self) -> None:
        self._pipeline = DataPipeline(
            name=self._config["name"], description=self._config["description"]
        )
        self._pipeline.context = self._context
        for _, step in self._steps.items():
            self._pipeline.add_step(step)


# ------------------------------------------------------------------------------------------------ #
#                                     DATASET PIPELINE                                             #
# ------------------------------------------------------------------------------------------------ #


class DatasetPipeline(Pipeline):
    """Pipeline for Dataset objects.

    Args:
        name (str): Human readable name for the pipeline run.
        description (str): Optional.
    """

    def __init__(self, name: str, description: str = None) -> None:
        super().__init__(name=name, description=description)

    def run(self) -> None:
        """Runs the pipeline"""
        self._data = None
        self._setup()
        for name, step in tqdm(self._steps.items()):

            result = step.run(data=self._data, context=self._context)
            self._data = result if result is not None else self._data

        self._teardown()


# ------------------------------------------------------------------------------------------------ #
class DatasetPipelineBuilder(PipelineBuilder):
    """Constructs the DatasetPipeline object."""

    def __init__(self) -> None:
        super().__init__()
        self.reset()
        self._config = None
        self._context = None
        self._steps = {}

    @property
    def config(self) -> dict:
        return self._config

    @property
    def context(self) -> Context:
        return self._context

    @property
    def steps(self) -> dict:
        return self._steps

    def build_config(self, config: dict) -> None:
        self._config = config

    def build_context(self, io: IOService) -> None:
        try:
            self._context = Context(
                name=self._config["name"], description=self._config["description"], io=io
            )
        except KeyError as e:
            logger.error(e)
            raise

    def build_steps(self) -> None:
        steps = self._config["steps"]

        for _, operator_name in steps.items():
            try:
                module = importlib.import_module(name=self._config["operator_module"])
                step = getattr(module, operator_name)

                operator = step()

                self._steps[operator.name] = deepcopy(operator)

            except KeyError as e:
                logging.error("Configuration File is missing operator configuration data")
                raise (e)

    def build_pipeline(self) -> None:
        self._pipeline = DatasetPipeline(
            name=self._config["name"], description=self._config["description"]
        )
        self._pipeline.context = self._context
        for _, step in self._steps.items():
            self._pipeline.add_step(step)
