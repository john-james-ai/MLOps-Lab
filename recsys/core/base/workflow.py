#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /workflow.py                                                                        #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday November 17th 2022 02:51:27 am                                             #
# Modified   : Friday November 18th 2022 08:54:13 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
from typing import Any, Union, List, Dict
from dataclasses import dataclass
import pandas as pd
from datetime import datetime
from abc import ABC, abstractmethod
import wandb


from recsys.core.services.logger import logger
from recsys.core.base.config import PROJECT, ENTITY
from recsys.core.services.io import IOService
from recsys.core.base.workflow import Context
from recsys.core.dal.dataset import DatasetParams, Dataset

# ------------------------------------------------------------------------------------------------ #


class Operator(ABC):
    """Abstract base class for pipeline operators.

    Note: All operator parameters in kwargs are added to the class as attributes.

    Args:
        name (str): The name for the operator that distinguishes it in the pipeline.
        description (str): A description for the operator instance. Optional
    """

    def __init__(self, **kwargs) -> None:
        self.name = None
        for k, v in kwargs.items():
            setattr(self, k, v)
        if not self.name:
            self.name = self.__class__.__name__.lower()

    def __str__(self) -> str:
        return f"{self.__class__.__name__}\n\tAttributes: {self.__dict__.items()}"

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}\n\tAttributes: {self.__dict__.items()}"

    @logger
    @abstractmethod
    def execute(self, data: pd.DataFrame = None, context: Context = None, **kwargs) -> Any:
        pass


# ------------------------------------------------------------------------------------------------ #
class DatasetOperator(ABC):
    """Base class for operators that interact with Dataset objects in the Dataset Repository.

    Input dataset parameters are queried by the repository decorator and the input dataset(s) are
    injected into the instance through the property setter. W.r.t the instance parameters,
    they are added to the class as attributes using setattr. Yeah, I know. Just got tired
    of typing get('param', None) from some parameters dictionary.

    Args:
        name (str): The name for the operator that distinguishes it in the pipeline.
        description (str): A description for the operator instance. Optional
        input_dataset_params (Union[DatasetParams], List[DatasetParams]) DatasetParams or a list
            thereof.
        output_dataset_params (Union[DatasetParams], List[DatasetParams]) DatasetParams or a list
            thereof.

    Attributes:
        input_dataset (Union[Dataset], List[Dataset]). The input dataset is injected by the
            repository decorator.

    Returns: Output Dataset Object
    """

    def __init__(
        self,
        input_dataset_params: Union[DatasetParams, List[DatasetParams]],
        output_dataset_params: Union[DatasetParams, List[DatasetParams]],
        **kwargs,
    ) -> None:
        self.name = None

        self.input_data = None
        self.output_data = None

        for k, v in kwargs.items():
            setattr(self, k, v)
        if not self.name:
            self.name = self.__class__.__name__.lower()

    def __str__(self) -> str:
        return f"{self.__class__.__name__}\n\tAttributes: {self.__dict__.items()}"

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}\n\tAttributes: {self.__dict__.items()}"

    @property
    def input_data(self) -> Union[Dataset, Dict[Dataset], List[Dataset]]:
        return self._input_data

    @input_data.setter
    def input_data(self, input_data: Union[Dataset, Dict[Dataset], List[Dataset]]) -> None:
        self._input_data = input_data

    @logger
    @abstractmethod
    def execute(self, context: Context, **kwargs) -> Any:
        pass


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

    def add_step(self, step: Operator) -> None:
        """Adds a step to the Pipeline object.

        Args:
            step: (Operator): Operator object to add to the pipeline.

        """
        self._steps[step.name] = step

    @logger
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
