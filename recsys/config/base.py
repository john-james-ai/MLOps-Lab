#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /base.py                                                                            #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday November 14th 2022 01:22:05 am                                               #
# Modified   : Thursday December 1st 2022 06:09:25 am                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Application-Wide and BaseConfiguration Module"""
from datetime import datetime
from dataclasses import dataclass
from abc import ABC
from typing import Union

# ------------------------------------------------------------------------------------------------ #
#                                DATA DIRECTORY STRUCTURE                                          #
# ------------------------------------------------------------------------------------------------ #
DATA_STRUCTURE = {
    "data": {
        "sources": {
            "movielens25m": {
                "ext": "data/sources/movielens25m/ext",
                "raw": "data/sources/movielens25m/raw",
                "prod": "data/sources/movielens25m/prod",
                "dev": "data/sources/movielens25m/dev",
                "test": "tests/data/sources/movielens25m/test",
            },
        },
        "repo": {
            "base": "data/repo",
            "registry": "data/repo/dataset.sqlite3"
            },
        },
        "archive": {
            "movielens25m": {
                "prod": "data/archive/movielens25m/prod",
                "dev": "data/archive/movielens25m/dev",
                "test": "tests/archive/movielens25m/test",
            }
        },
    }
}

# ------------------------------------------------------------------------------------------------ #
#                                    REPRODUCIBILITY                                               #
# ------------------------------------------------------------------------------------------------ #
RANDOM_STATE = 55
# ------------------------------------------------------------------------------------------------ #
#                                    TRAIN/TEST SPLIT                                              #
# ------------------------------------------------------------------------------------------------ #
TRAIN_PROPORTION = 0.8
# ------------------------------------------------------------------------------------------------ #
#                                      DATA TYPES                                                  #
# ------------------------------------------------------------------------------------------------ #
IMMUTABLE_TYPES: tuple = (str, int, float, bool, type(None))
SEQUENCE_TYPES: tuple = (list, tuple)

# ------------------------------------------------------------------------------------------------ #
#                                    BASE CONFIG                                                   #
# ------------------------------------------------------------------------------------------------ #


@dataclass
class Config(ABC):
    """Configuration Base Class"""

    def as_dict(self) -> dict:
        """Returns a dictionary representation of the the Config object."""
        return {k.lstrip("_"): self._export_config(v) for k, v in self.__dict__.items()}

    @classmethod
    def _export_config(cls, v):
        """Returns v with Configs converted to dicts, recursively."""
        if isinstance(v, IMMUTABLE_TYPES):
            return v
        elif isinstance(v, SEQUENCE_TYPES):
            return type(v)(map(cls._export_config, v))
        elif isinstance(v, datetime):
            return v.strftime("%H:%M:%S on %m/%d/%Y")
        elif isinstance(v, dict):
            return v
        else:
            """Else nothing. What do you want?"""


# ------------------------------------------------------------------------------------------------ #
#                            WORKFLOW PARAMETER OBJECT BASE CLASSES                                #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class StepPO(Config):
    """Base class for step parameter objects. These parameters control operator behavior."""


# ------------------------------------------------------------------------------------------------ #
@dataclass
class FilesetPO(Config):
    """Name and ID for a Dataset."""

    source: str = None
    filepath: str = None


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DatasetPO(Config):
    name: str = None
    source: str = None
    env: str = None
    stage: str = None


# ------------------------------------------------------------------------------------------------ #
@dataclass
class OperatorParams(Config):
    step_params: StepPO
    input_params: Union[FilesetPO, DatasetPO]
    output_params: Union[FilesetPO, DatasetPO]
