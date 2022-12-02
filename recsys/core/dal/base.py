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
# Created    : Thursday December 1st 2022 05:44:55 am                                              #
# Modified   : Friday December 2nd 2022 02:43:09 am                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Base Data Access Module"""
from datetime import datetime
from dataclasses import dataclass
import logging
from abc import ABC, abstractmethod
from typing import Any

from recsys.config import IMMUTABLE_TYPES, SEQUENCE_TYPES

# ------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #


# ------------------------------------------------------------------------------------------------ #
#                            DATA ACCESS OBJECT ABC                                                #
# ------------------------------------------------------------------------------------------------ #


class DAO(ABC):
    @abstractmethod
    def add(self, *args, **kwargs) -> None:
        """Adds a dataset to the registry. If a duplicate is found, the version is bumped"""

    @abstractmethod
    def get(self, id: int) -> Any:
        """Retrieves dataset metadata from the registry, given an id

        Args:
            id (int): The id for the Dataset to retrieve.
        """

    @abstractmethod
    def get_all(self) -> Any:
        """Returns a Dataframe representation of the registry."""

    @abstractmethod
    def exists_id(self, id: int) -> bool:
        """Returns True if the entity with id exists in the database."""

    @abstractmethod
    def exists(self, *args, **kwargs) -> bool:
        """Returns true if a dataset with the same name, stage and version exists in the registry."""

    @abstractmethod
    def remove(self, id: int) -> None:
        """Deletes a Dataset from the registry, given an id.

        Args:
            id (int): The id for the Dataset to remove.
        """


# ------------------------------------------------------------------------------------------------ #
#                            DATA TRANSFER OBJECT ABC                                              #
# ------------------------------------------------------------------------------------------------ #


@dataclass
class DTO(ABC):
    """Data Transfer Object"""

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
            return v
        else:
            """Else nothing. What do you want?"""
