#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /prod.py                                                                            #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday November 23rd 2022 07:48:55 pm                                            #
# Modified   : Wednesday November 23rd 2022 11:56:43 pm                                            #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Collaborative Filtering Development Environment Preprocessing."""

from dataclasses import dataclass
from recsys.config.base import (
    StepParams,
    DatasetInput,
    DatasetOutput,
    OperatorParams,
)

# ================================================================================================ #
#                                     COLLABORATIVE FILTERING                                      #
# ================================================================================================ #
# ------------------------------------------------------------------------------------------------ #
#                                         PREPROCESSING                                            #
# ------------------------------------------------------------------------------------------------ #


@dataclass
class CreateDatasetStep(StepParams):
    name: str = "create_rating_dataset"
    description: str = "Creates rating dataset"
    module: str = "recsys.core.workflow.operators"
    operator: str = "CreateDataset"


# ------------------------------------------------------------------------------------------------ #


@dataclass
class CreateDatasetInput(DatasetInput):
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
