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
# Modified   : Thursday November 24th 2022 02:44:29 am                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Application-Wide and BaseConfiguration Module"""
from datetime import datetime
from dataclasses import dataclass
from abc import ABC

# ------------------------------------------------------------------------------------------------ #
#                                ENVIRONMENT AND STAGE                                             #
# ------------------------------------------------------------------------------------------------ #
ENVS = ["dev", "prod", "test"]
STAGES = ["raw", "interim", "cooked"]
# ------------------------------------------------------------------------------------------------ #
#                                    DATASET CONFIG                                                #
# ------------------------------------------------------------------------------------------------ #
DATASET_FEATURES = [
    "name",
    "description",
    "env",
    "stage",
    "version",
    "cost",
    "nrows",
    "ncols",
    "null_counts",
    "memory_size",
    "filepath",
    "creator",
    "created",
]
# ------------------------------------------------------------------------------------------------ #
#                                     REPO CONFIG                                                  #
# ------------------------------------------------------------------------------------------------ #
REPO_FILE_FORMAT = "pkl"
REPO_DIRS = {
    "data": {
        "dev": "data/movielens20m/repo",
        "prod": "data/movielens20m/repo",
        "test": "tests/data/movielens20m/repo",
    },
    "model": {
        "dev": "models/movielens20m/repo",
        "prod": "models/movielens20m/repo",
        "test": "tests/models/movielens20m/repo",
    },
}
# ------------------------------------------------------------------------------------------------ #
#                                     DB CONFIG                                                    #
# ------------------------------------------------------------------------------------------------ #
DB_TABLES = {"DatasetRegistry": "dataset_registry"}
DB_LOCATIONS = {
    "data": {
        "dev": "data/movielens20m/repo/dataset_registry.sqlite",
        "prod": "data/movielens20m/repo/dataset_registry.sqlite",
        "test": "tests/data/movielens20m/repo/dataset_registry.sqlite",
    },
    "model": {
        "dev": "models/repo/model_registry.sqlite",
        "prod": "models/repo/model_registry.sqlite",
        "test": "tests/models/repo/model_registry.sqlite",
    },
}

# ------------------------------------------------------------------------------------------------ #
#                                 PIPELINE CONFIGS                                                 #
# ------------------------------------------------------------------------------------------------ #
PIPELINES = {
    "collabfilter": {
        "preprocess": {
            "dev": "recsys.config.collabfilter.dev",
            "prod": "recsys.config.collabfilter.prod",
            "test": "recsys.config.collabfilter.test",
        },
    }
}

# ------------------------------------------------------------------------------------------------ #
#                                  BASE CONFIG CLASS                                               #
# ------------------------------------------------------------------------------------------------ #
# ------------------------------------------------------------------------------------------------ #
#                            DATA TYPES AND FILE FORMATS                                           #
# ------------------------------------------------------------------------------------------------ #
IMMUTABLE_TYPES: tuple = (str, int, float, bool, type(None))
SEQUENCE_TYPES: tuple = (list, tuple)
COMPRESSED_FILE_FORMATS = ("tar.gz", "zip", "7z")


@dataclass
class Config(ABC):
    """Configuration Base Class"""

    def as_dict(self) -> dict:
        """Returns a dictionary representation of the the Config object."""
        return {k: self._export_config(v) for k, v in self.__dict__.items()}

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
            return {kk: cls._export_config(vv) for kk, vv in v}
        else:
            pass


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
