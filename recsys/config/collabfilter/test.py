#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /test.py                                                                            #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday November 23rd 2022 07:48:55 pm                                            #
# Modified   : Thursday November 24th 2022 04:44:13 am                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Collaborative Filtering Development Environment Preprocessing."""
from dataclasses import dataclass, field
from typing import Dict

from recsys.config.base import (
    StepParams,
    FilesetInput,
    DatasetInput,
    DatasetOutput,
    OperatorParams,
)

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


# ------------------------------------------------------------------------------------------------ #


@dataclass
class CreateDatasetInput(FilesetInput):
    name: str = "rating"
    filepath: str = "tests/data/etl/movielens20m/raw/rating.pkl"


# ------------------------------------------------------------------------------------------------ #
@dataclass
class CreateDatasetOutput(DatasetOutput):
    name: str = "rating"
    description: str = "Rating Dataset"
    env: str = "test"
    stage: str = "interim"
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
    train_proportion: float = 0.8
    random_state: int = 55


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
    description: str = "Train Set 0.8 Clustered on UserID"
    env: str = "test"
    stage: str = "interim"
    version: int = 1


# ------------------------------------------------------------------------------------------------ #
@dataclass
class Test(DatasetOutput):
    """Train Set"""

    name: str = "test"
    description: str = "Test Set 0.2 Clustered on UserID"
    env: str = "test"
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
class RatingsAdjusterStep(StepParams):
    name: str = "ratings_adjuster"
    description: str = "Center Ratings by User Mean"
    module: str = "recsys.core.workflow.operators"
    operator: str = "RatingsAdjuster"


# ------------------------------------------------------------------------------------------------ #
@dataclass
class RatingsAdjusterInput(DatasetInput):
    """Dictionary of string Dataset id pairs."""

    id: int = 2
    name: str = "train"


# ------------------------------------------------------------------------------------------------ #
@dataclass
class RatingsAdjusterOutput(DatasetOutput):
    """Adjusted Ratings"""

    name: str = "adjusted_ratings"
    description: str = "Centered Ratings on Average User Rating"
    env: str = "test"
    stage: str = "interim"
    version: int = 1


# ------------------------------------------------------------------------------------------------ #
@dataclass
class RatingsAdjusterParams(OperatorParams):
    step_params: StepParams = RatingsAdjusterStep()
    input_params: DatasetInput = RatingsAdjusterInput()
    output_params: DatasetOutput = RatingsAdjusterOutput()


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
    env: str = "test"
    stage: str = "interim"
    version: int = 1


# ------------------------------------------------------------------------------------------------ #
@dataclass
class UserParams(OperatorParams):
    step_params: StepParams = UserStep()
    input_params: DatasetInput = UserInput()
    output_params: DatasetOutput = UserOutput()


# ------------------------------------------------------------------------------------------------ #
#                                          PHI                                                     #
# ------------------------------------------------------------------------------------------------ #


@dataclass
class PhiStep(StepParams):
    name: str = "phi"
    description: str = "Creates all User-User Combinations"
    module: str = "recsys.core.workflow.operators"
    operator: str = "Phi"


# ------------------------------------------------------------------------------------------------ #
@dataclass
class PhiInput(DatasetInput):
    """Dictionary of string Dataset id pairs."""

    id: int = 3
    name: str = "adjusted_ratings"


# ------------------------------------------------------------------------------------------------ #
@dataclass
class PhiOutput(DatasetOutput):
    """User-User Combinations"""

    name: str = "user"
    description: str = "User-User Combinations"
    env: str = "test"
    stage: str = "interim"
    version: int = 1


# ------------------------------------------------------------------------------------------------ #
@dataclass
class PhiParams(OperatorParams):
    step_params: StepParams = PhiStep()
    input_params: DatasetInput = PhiInput()
    output_params: DatasetOutput = PhiOutput()
