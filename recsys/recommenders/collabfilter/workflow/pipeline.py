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
# Created    : Sunday November 27th 2022 06:59:18 am                                               #
# Modified   : Sunday November 27th 2022 04:50:26 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import importlib
import logging
from tqdm import tqdm

from recsys.core.services.io import IOService
from recsys.core.workflow.pipeline import Pipeline, PipelineBuilder, Context


# ------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #


class CFDataPipeline(Pipeline):
    """Collaborative Filtering Data Preparation Pipeline

    This pipeline performs train / test split, and
    data preprocessing of the user rating data such
    as centering the ratings, and computing user
    average ratings.

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
class CFDataPipelineBuilder(PipelineBuilder):
    """Constructs the CFDataPipeline object."""

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

                self._steps[operator.name] = operator

            except KeyError as e:
                logging.error("Configuration File is missing operator configuration data")
                raise (e)

    def build_pipeline(self) -> None:
        logger.debug(
            f"Config name: {self._config['name']}, Config description: {self._config['description']}"
        )
        self._pipeline = CFDataPipeline(
            name=self._config["name"], description=self._config["description"]
        )
        self._pipeline.context = self._context
        for _, step in self._steps.items():
            self._pipeline.add_step(step)
