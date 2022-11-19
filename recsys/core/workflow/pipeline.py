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
# Modified   : Saturday November 19th 2022 03:50:44 pm                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
from dataclasses import dataclass
from abc import ABC, abstractmethod
from typing import Any
import logging
import importlib
from datetime import datetime
from tqdm import tqdm
import wandb

from recsys.core.services.io import IOService
from recsys.core.services.profiler import profiler
from recsys.core.base.config import PROJECT, ENTITY

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

        self._created = datetime.now()
        self._started = None
        self._ended = None
        self._duration = None
        self._wandb_run = None

        self._context = None

    @property
    def name(self) -> str:
        return self._name

    @property
    def created(self) -> datetime:
        return self._created

    @property
    def started(self) -> datetime:
        return self._started

    @property
    def ended(self) -> datetime:
        return self._ended

    @property
    def duration(self) -> datetime:
        return self._duration

    @property
    def steps(self) -> list:
        return self._steps

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
        self._wandb_run = wandb.init(project=PROJECT, entity=ENTITY, name=self._name, reinit=True)
        self._started = datetime.now()

    def _teardown(self) -> None:
        """Completes the pipeline process."""
        self._ended = datetime.now()
        self._duration = (self._ended - self._started).total_seconds()
        wandb.log(
            {
                "pipeline": self._name,
                "started": self._started,
                "ended": self._ended,
                "duration": self._duration,
            }
        )
        self._wandb_run.finish()


# ------------------------------------------------------------------------------------------------ #


class Pipeline(Pipeline):
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
        self._setup()
        for name, step in tqdm(self._steps.items()):
            started = datetime.now()

            step.execute(context=self._context)

            ended = datetime.now()
            duration = (ended - started).total_seconds()
            wandb.log(
                {
                    "pipeline": self._context.name,
                    "step": name,
                    "started": started,
                    "ended": ended,
                    "duration": duration,
                }
            )
        self._teardown()


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
    def build_context(self) -> None:
        pass

    @abstractmethod
    def build_steps(self) -> None:
        pass

    @abstractmethod
    def build_pipeline(self) -> None:
        pass


# ------------------------------------------------------------------------------------------------ #
class PipelineBuilder(PipelineBuilder):
    """Constructs an  processing pipeline."""

    def __init__(self) -> None:
        self.reset()
        self._config = None
        self._context = None
        self._steps = {}

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

                operator = step(**step_config["params"])

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


class PipelineDirector(ABC):
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
class PipelineDirector(PipelineDirector):
    """Pipelne director responsible for executing the steps of the PipelineBuilder in a sequence.

    Args:
        config_filepath (str): The path to the pipeline configuration file
        builder (PipelineBuilder): The concrete builder class
    """

    def __init__(self, config_filepath: str, builder: PipelineBuilder) -> None:
        super().__init__(config_filepath=config_filepath, builder=builder)

    def build_etl_pipeline(self) -> None:
        """Constructs the  Pipeline"""
        self._builder.build_config(config=self._config)
        self._builder.build_context()
        self._builder.build_steps()
        self._builder.build_pipeline()
