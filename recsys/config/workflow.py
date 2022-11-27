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
# Created    : Friday November 25th 2022 06:32:02 pm                                               #
# Modified   : Friday November 25th 2022 06:32:44 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
from dataclasses import dataclass
from .base import Config


# ================================================================================================ #
#                                    WORKFLOW CONFIG                                               #
# ================================================================================================ #
# ------------------------------------------------------------------------------------------------ #
#                                PIPELINE STEP PARAMETERS                                          #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class StepParams(Config):
    name: str
    description: str
    module: str
    operator: str


# ------------------------------------------------------------------------------------------------ #
#                                  INPUT DATASET PARAMETERS                                        #
# ------------------------------------------------------------------------------------------------ #


@dataclass
class DatasetInput(Config):
    """Name and ID for a Dataset."""

    id: int
    name: str


@dataclass
class FilesetInput(Config):
    """Name and ID for a Dataset."""

    name: str
    filepath: str


# ------------------------------------------------------------------------------------------------ #
#                                 OUTPUT DATASET PARAMETERS                                        #
# ------------------------------------------------------------------------------------------------ #


@dataclass
class DatasetOutput(Config):
    """Dataset Passport"""

    name: str
    description: str
    env: str
    stage: str
    version: int


# ------------------------------------------------------------------------------------------------ #
#                                   OPERATOR PARAMETERS                                            #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class OperatorParams(Config):
    step_params: StepParams
    input_params: DatasetInput
    output_params: DatasetOutput


# ================================================================================================ #
#                                    VISUAL CONFIG                                                 #
# ================================================================================================ #
@dataclass
class VisualConfig(Config):
    figsize: tuple = (12, 6)
    darkblue: str = "#1C3879"
    lightblue: str = "steelblue"
    palette: str = "Blues_r"
    style: str = "whitegrid"
