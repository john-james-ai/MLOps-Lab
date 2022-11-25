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
# Modified   : Friday November 25th 2022 02:20:59 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Application-Wide and BaseConfiguration Module"""
import os
from datetime import datetime
from dataclasses import dataclass
from abc import ABC

# ------------------------------------------------------------------------------------------------ #
#                                     REPRODUCIBILITY                                              #
# ------------------------------------------------------------------------------------------------ #
RANDOM_STATE = 55

# ------------------------------------------------------------------------------------------------ #
#                                ENVIRONMENT AND STAGE                                             #
# ------------------------------------------------------------------------------------------------ #
ENVS = ["dev", "prod", "test"]
STAGES = ["input", "interim", "cooked"]
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
#                                    DIRECTORY CONFIG                                              #
# ------------------------------------------------------------------------------------------------ #
DIRECTORIES = {
    "data": {"base": "data", "ext": "data/ext", "raw": "data/raw", "repo": "data/repo"},
    "models": {"base": "models", "repo": "models/repo"},
}
# ------------------------------------------------------------------------------------------------ #
#                                     REPO CONFIG                                                  #
# ------------------------------------------------------------------------------------------------ #
REPO_FILE_FORMAT = "pkl"
REPO_DIRS = {
    "data": {
        "dev": os.path.join(DIRECTORIES["data"]["repo"], "dev"),
        "prod": os.path.join(DIRECTORIES["data"]["repo"], "prod"),
        "test": os.path.join(DIRECTORIES["data"]["repo"], "test"),
    },
    "models": {
        "dev": os.path.join(DIRECTORIES["models"]["repo"], "dev"),
        "prod": os.path.join(DIRECTORIES["models"]["repo"], "prod"),
        "test": os.path.join(DIRECTORIES["models"]["repo"], "test"),
    },
}
# ------------------------------------------------------------------------------------------------ #
#                                     DB CONFIG                                                    #
# ------------------------------------------------------------------------------------------------ #
DB_TABLES = {"DatasetRegistry": "dataset_registry"}
DB_LOCATIONS = {
    "data": {
        "dev": os.path.join(REPO_DIRS["data"]["dev"], "dataset_registry.sqlite"),
        "prod": os.path.join(REPO_DIRS["data"]["prod"], "dataset_registry.sqlite"),
        "test": os.path.join(REPO_DIRS["data"]["test"], "dataset_registry.sqlite"),
    },
    "model": {
        "dev": os.path.join(REPO_DIRS["models"]["dev"], "dataset_registry.sqlite"),
        "prod": os.path.join(REPO_DIRS["models"]["prod"], "dataset_registry.sqlite"),
        "test": os.path.join(REPO_DIRS["models"]["test"], "dataset_registry.sqlite"),
    },
}

# ------------------------------------------------------------------------------------------------ #
#                            DATA TYPES AND FILE FORMATS                                           #
# ------------------------------------------------------------------------------------------------ #
IMMUTABLE_TYPES: tuple = (str, int, float, bool, type(None))
SEQUENCE_TYPES: tuple = (list, tuple)
COMPRESSED_FILE_FORMATS = ("tar.gz", "zip", "7z")

# ------------------------------------------------------------------------------------------------ #
#                             SAMPLING AND TRAIN PROPORTIONS                                       #
# ------------------------------------------------------------------------------------------------ #

SAMPLE_PROPORTION = {"prod": 1.0, "dev": 0.1, "test": 0.01}
TRAIN_PROPORTION = 0.8

# ------------------------------------------------------------------------------------------------ #
#                                    BASE CONFIG                                                   #
# ------------------------------------------------------------------------------------------------ #


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
