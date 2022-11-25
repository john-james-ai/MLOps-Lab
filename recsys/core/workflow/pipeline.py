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
# Modified   : Wednesday November 23rd 2022 02:34:35 pm                                            #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
from dataclasses import dataclass
from abc import ABC, abstractmethod
from typing import Any
import logging
import importlib
from tqdm import tqdm

from recsys.core.services.io import IOService
from recsys.core.services.profiler import profiler


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

    name: str
    description: str = None
    io: Any = None


# ------------------------------------------------------------------------------------------------ #
class PipelineABC(ABC):
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

    @profiler
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


class Pipeline(PipelineABC):
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


class PipelineBuilderABC(ABC):
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
    def build_context(self) -> None:
        pass

    @abstractmethod
    def build_steps(self) -> None:
        pass

    @abstractmethod
    def build_pipeline(self) -> None:
        pass


# ------------------------------------------------------------------------------------------------ #
class PipelineBuilder(PipelineBuilderABC):
    """Constructs an  processing pipeline."""

    def __init__(self) -> None:
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

    def build_context(self) -> None:
        try:
            module = importlib.import_module(name=self._config["io_module"])
            io = getattr(module, self._config["io_service"])
            self._context = Context(
                name=self._config["name"], description=self._config["description"], io=io
            )
        except KeyError as e:
            logger.error(e)
            raise

    def build_steps(self) -> None:
        steps = self._config["steps"]
        for _, step_config in steps.items():
            try:
                module = importlib.import_module(name=step_config["module"])
                step = getattr(module, step_config["operator"])

                operator = step(
                    name=step_config["name"],
                    description=step_config["description"],
                    **step_config["params"],
                )

                logger.debug(operator)

                self._steps[operator.name] = operator

            except KeyError as e:
                logging.error("Configuration File is missing operator configuration data")
                raise (e)

    def build_pipeline(self) -> None:
        logger.debug(
            f"Config name: {self._config['name']}, Config description: {self._config['description']}"
        )
        self._pipeline = Pipeline(
            name=self._config["name"], description=self._config["description"]
        )
        logger.debug(self._pipeline)
        self._pipeline.context = self._context
        for _, step in self._steps.items():
            self._pipeline.add_step(step)


# ------------------------------------------------------------------------------------------------ #


class PipelineDirectorABC(ABC):
    """Pipelne director responsible for executing the steps of the PipelineBuilder in a sequence.

    Args:
        config_filepath (str): The path to the pipeline configuration file
        builder (PipelineBuilder): The concrete builder class
    """

    def __init__(self, config_filepath: str, builder: PipelineBuilder) -> None:
        self._config = IOService.read(config_filepath)
        self._builder = builder

    @property
    def builder(self) -> PipelineBuilder:
        return self._builder

    @builder.setter
    def builder(self, builder: PipelineBuilder) -> None:
        self._builder = builder


# ------------------------------------------------------------------------------------------------ #
class PipelineDirector(PipelineDirectorABC):
    """Pipelne director responsible for executing the steps of the PipelineBuilder in a sequence.

    Args:
        config_filepath (str): The path to the pipeline configuration file
        builder (PipelineBuilder): The concrete builder class
    """

    def __init__(self, config_filepath: str, builder: PipelineBuilder) -> None:
        super().__init__(config_filepath=config_filepath, builder=builder)

    def build_pipeline(self) -> None:
        """Constructs the  Pipeline"""
        self._builder.build_config(config=self._config)
        self._builder.build_context()
        self._builder.build_steps()
        self._builder.build_pipeline()
