#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/workflow/pipeline.py                                                   #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday December 5th 2022 03:48:39 am                                                #
# Modified   : Tuesday December 6th 2022 01:12:09 am                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import importlib

from tqdm import tqdm

from .base import Pipeline, PipelineBuilder, Context
from recsys.core.services.io import IOService


# ------------------------------------------------------------------------------------------------ #
#                                    DATA PIPELINE                                                 #
# ------------------------------------------------------------------------------------------------ #


class DataPipeline(Pipeline):
    """Pipeline class
    Executes Extract-Transform-Load pipelines
    Args:
        name (str): Human readable name for the pipeline run.
        description (str): Optional.
    """

    def __init__(
        self, name: str, source: str, workspace: str, description: str = None, **kwargs
    ) -> None:
        super().__init__(name=name, source=source, workspace=workspace, description=description)

    def run(self) -> None:
        """Runs the pipeline"""
        self._data = None
        self._setup()
        for name, task in tqdm(self._tasks.items()):

            result = task.run(data=self._data, context=self._context)
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
        self._tasks = {}

    @property
    def config(self) -> dict:
        return self._config

    @property
    def context(self) -> Context:
        return self._context

    @property
    def tasks(self) -> dict:
        return self._tasks

    def build_config(self, config: dict) -> None:
        self._config = config

    def build_context(self, io: IOService) -> None:
        try:
            self._context = Context(
                name=self._config["name"], description=self._config["description"], io=io
            )
        except KeyError as e:  # pragma: no cover
            self._logger.error(e)
            raise

    def build_tasks(self) -> None:
        tasks = self._config["tasks"]
        for task in tasks:
            try:
                module = importlib.import_module(name=task_config["module"])
                task = getattr(module, task_config["operator"])

                operator = task(
                    name=task_config["name"],
                    description=task_config["description"],
                    specs=task_config["specs"],
                    input=task_config["input"],
                    output=[output for output in task_config["output"]],
                    force=task_config["force"],
                )

                self._tasks[operator.name] = operator

            except KeyError as e:  # pragma: no cover
                self._logger.error("Configuration File is missing operator configuration data")
                raise (e)

    def build_pipeline(self) -> None:
        self._pipeline = DataPipeline(
            name=self._config["name"], description=self._config["description"]
        )
        self._pipeline.context = self._context
        for _, task in self._tasks.items():
            self._pipeline.add_task(task)
