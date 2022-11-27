#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /preprocess.py                                                                      #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday November 25th 2022 11:26:57 am                                               #
# Modified   : Saturday November 26th 2022 12:42:35 pm                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import os
from dataclasses import dataclass, field
from typing import Dict
from dotenv import load_dotenv

from recsys.config.workflow import (
    StepParams,
    FilesetInput,
    DatasetInput,
    DatasetOutput,
    OperatorParams,
)
from recsys.config.base import DATA_DIRS, SAMPLE_PROPORTION, TRAIN_PROPORTION, RANDOM_STATE

# ================================================================================================ #
#                                        PREPROCESSING                                             #
# ================================================================================================ #
# ------------------------------------------------------------------------------------------------ #
#                                        CREATE DATASET                                            #
# ------------------------------------------------------------------------------------------------ #


@dataclass
class CreateDatasetStep(StepParams):
    name: str = "create_rating_dataset"
    description: str = "Creates rating dataset"
    module: str = "recsys.core.workflow.operators"
    operator: str = "CreateDataset"
    sample: bool = False
    clustered_by: str = "userId"
    clustered: bool = True
    frac: float = None
    random_state: int = RANDOM_STATE

    def __post_init__(self) -> None:
        load_dotenv()
        ENV = os.getenv("ENV")
        self.frac = SAMPLE_PROPORTION[ENV]
        if "prod" not in ENV:
            self.sample = True


# ------------------------------------------------------------------------------------------------ #


@dataclass
class CreateDatasetInput(FilesetInput):
    name: str = "rating"
    filename = "rating.csv"
    filepath: str = None

    def __post_init__(self) -> None:
        self.filepath = os.path.join(DATA_DIRS["input"], self.filename)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class CreateDatasetOutput(DatasetOutput):
    name: str = "rating"
    description: str = "Rating Dataset"
    stage: str = "input"
    version: int = 1


# ------------------------------------------------------------------------------------------------ #
@dataclass
class CreateDatasetParams(OperatorParams):
    step_params: StepParams = CreateDatasetStep()
    input_params: DatasetInput = CreateDatasetInput()
    output_params: DatasetOutput = CreateDatasetOutput()


# ------------------------------------------------------------------------------------------------ #
#                                   TRAIN TEST SPLIT                                               #
# ------------------------------------------------------------------------------------------------ #


@dataclass
class TrainTestStep(StepParams):
    name: str = "train_test_split"
    description: str = "User Clustered Train-Test Split"
    module: str = "recsys.core.workflow.operators"
    operator: str = "TrainTestSplit"
    clustered_by: str = "userId"
    clustered: bool = True
    train_proportion: float = TRAIN_PROPORTION
    random_state: int = RANDOM_STATE


# ------------------------------------------------------------------------------------------------ #
@dataclass
class TrainTestInput(DatasetInput):
    """Dictionary of string Dataset id pairs."""

    id: int = 1
    name: str = "rating"


# ------------------------------------------------------------------------------------------------ #
@dataclass
class Train(DatasetOutput):
    """Train Set"""

    name: str = "train"
    description: str = "Train Set"
    stage: str = "interim"
    version: int = 1


# ------------------------------------------------------------------------------------------------ #
@dataclass
class Test(DatasetOutput):
    """Train Set"""

    name: str = "test"
    description: str = "Test Set"
    stage: str = "interim"
    version: int = 1


# ------------------------------------------------------------------------------------------------ #
@dataclass
class TrainTestParams(OperatorParams):
    step_params: StepParams = TrainTestStep()
    input_params: DatasetInput = TrainTestInput()
    output_params: Dict[str, DatasetOutput] = field(
        default_factory=lambda: ({"train": Train, "test": Test})
    )


# ------------------------------------------------------------------------------------------------ #
#                                   CENTER RATINGS                                                 #
# ------------------------------------------------------------------------------------------------ #


@dataclass
class DataCentralizerStep(StepParams):
    name: str = "ratings_adjuster"
    description: str = "Center Ratings by User Mean"
    module: str = "recsys.core.workflow.operators"
    operator: str = "DataCentralizer"


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DataCentralizerInput(DatasetInput):
    """Dictionary of string Dataset id pairs."""

    id: int = 2
    name: str = "train"


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DataCentralizerOutput(DatasetOutput):
    """Adjusted Ratings"""

    name: str = "adjusted_ratings"
    description: str = "Centered Ratings on Average User Rating"
    stage: str = "interim"
    version: int = 1


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DataCentralizerParams(OperatorParams):
    step_params: StepParams = DataCentralizerStep()
    input_params: DatasetInput = DataCentralizerInput()
    output_params: DatasetOutput = DataCentralizerOutput()


# ------------------------------------------------------------------------------------------------ #
#                                   USER AVE RATINGS                                               #
# ------------------------------------------------------------------------------------------------ #


@dataclass
class UserStep(StepParams):
    name: str = "user"
    description: str = "User Average Ratings"
    module: str = "recsys.core.workflow.operators"
    operator: str = "User"


# ------------------------------------------------------------------------------------------------ #
@dataclass
class UserInput(DatasetInput):
    """Dictionary of string Dataset id pairs."""

    id: int = 2
    name: str = "train"


# ------------------------------------------------------------------------------------------------ #
@dataclass
class UserOutput(DatasetOutput):
    """Adjusted Ratings"""

    name: str = "user"
    description: str = "User Average Ratings"
    stage: str = "interim"
    version: int = 1


# ------------------------------------------------------------------------------------------------ #
@dataclass
class UserParams(OperatorParams):
    step_params: StepParams = UserStep()
    input_params: DatasetInput = UserInput()
    output_params: DatasetOutput = UserOutput()
