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
# Created    : Friday December 2nd 2022 02:26:50 am                                                #
# Modified   : Friday December 2nd 2022 02:28:50 am                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Application-Wide and BaseConfiguration Module"""
from datetime import datetime
from dataclasses import dataclass
from abc import ABC


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
