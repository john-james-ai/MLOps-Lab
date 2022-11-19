#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /config.py                                                                          #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday November 14th 2022 01:22:05 am                                               #
# Modified   : Friday November 18th 2022 11:10:30 pm                                               #
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
#                                      DATA TYPES                                                  #
# ------------------------------------------------------------------------------------------------ #
IMMUTABLE_TYPES: tuple = (str, int, float, bool, type(None))
SEQUENCE_TYPES: tuple = (list, tuple)
# ------------------------------------------------------------------------------------------------ #
#                                       FILE FORMATS                                               #
# ------------------------------------------------------------------------------------------------ #
FILE_FORMATS = ("csv", "yml", "yaml", "pickle", "pkl", "nii", "nib", "dcm", "h5")
COMPRESSED_FILE_FORMATS = ("tar.gz", "zip", "7z")
DATA_REPO_FILE_FORMAT = "pkl"

# ------------------------------------------------------------------------------------------------ #
#                                    DATASET VARIABLES                                             #
# ------------------------------------------------------------------------------------------------ #
ENVS = ["dev", "prod", "test"]
STAGES = ["raw", "interim", "cooked"]
# ------------------------------------------------------------------------------------------------ #
#                                       DIRECTORIES                                                #
# ------------------------------------------------------------------------------------------------ #
BASE_DATA_DIR = "data"
DATA_REPO_DIR = os.path.join(BASE_DATA_DIR, "dataset/repo")

BASE_MODEL_DIR = "models"
MODEL_REPO_DIR = os.path.join(BASE_MODEL_DIR, "repo")
# ------------------------------------------------------------------------------------------------ #
#                                      WANDB CONFIG                                                #
# ------------------------------------------------------------------------------------------------ #
PROJECT = "recsys"
ENTITY = "aistudio"

# ------------------------------------------------------------------------------------------------ #
#                                   BASE CONFIG CLASS                                              #
# ------------------------------------------------------------------------------------------------ #


@dataclass
class Config(ABC):
    name: str = None
    test: bool = False

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


# ------------------------------------------------------------------------------------------------ #
#                                    VISUALIZATION CONFIG                                          #
# ------------------------------------------------------------------------------------------------ #


@dataclass
class VisualConfig(Config):
    figsize: tuple = (12, 6)
    darkblue: str = "#1C3879"
    lightblue: str = "steelblue"
    palette: str = "Blues_r"
    style: str = "whitegrid"
