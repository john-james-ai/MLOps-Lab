#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /etl.py                                                                             #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday November 25th 2022 02:57:23 pm                                               #
# Modified   : Saturday November 26th 2022 05:33:24 am                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import importlib
import logging

from tqdm import tqdm

from recsys.core.workflow.pipeline import Pipeline, PipelineBuilder, Context
from recsys.core.services.io import IOService

# ------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #


class ETLPipeline(Pipeline):
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


class ETLPipelineBuilder(PipelineBuilder):
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
        self._pipeline = ETLPipeline(
            name=self._config["name"], description=self._config["description"]
        )
        logger.debug(self._pipeline)
        self._pipeline.context = self._context
        for _, step in self._steps.items():
            self._pipeline.add_step(step)
