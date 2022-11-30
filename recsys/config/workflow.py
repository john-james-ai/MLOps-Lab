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
# Modified   : Tuesday November 29th 2022 08:35:27 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
from abc import ABC
from dataclasses import dataclass
from .base import Config
from recsys.core.dal.dataset import Dataset


# ================================================================================================ #
#                                    WORKFLOW CONFIG                                               #
# ================================================================================================ #
RANDOM_STATE = 55
# ------------------------------------------------------------------------------------------------ #
#                                   PIPELINE STEP PARAMETERS                                       #
# ------------------------------------------------------------------------------------------------ #


@dataclass
class StepPO(Config):
    force: bool


# ------------------------------------------------------------------------------------------------ #
#                                      INPUT PARAMETERS                                            #
# ------------------------------------------------------------------------------------------------ #


@dataclass
class InputPO(Config):
    """Base class for Input Parameter Objects"""


# ------------------------------------------------------------------------------------------------ #
@dataclass
class FilesetInputPO(InputPO):
    """Name and ID for a Dataset."""

    filepath: str


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DatasetInputPO(InputPO):
    name: str
    stage: str


# ------------------------------------------------------------------------------------------------ #
#                                     OUTPUT PARAMETERS                                            #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class OutputPO(Config):
    """Base class for Output Parameter Objects."""


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DatasetOutputPO(OutputPO):
    """Dataset Output Parameter Object."""

    name: str
    description: str
    stage: str


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DatasetGroupPO(OutputPO):
    """Base class for a group of Parameter Objects."""


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DatasetGroupABC(ABC):
    train: Dataset
    test: Dataset


# ------------------------------------------------------------------------------------------------ #
#                       DATASET OPERATOR PARAMETER OBJECT GROUP                                    #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class OperatorParams(Config):
    step_params: StepPO
    input_params: InputPO
    output_params: OutputPO
