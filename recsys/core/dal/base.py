#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/dal/base.py                                                            #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday December 3rd 2022 12:44:06 pm                                              #
# Modified   : Sunday December 4th 2022 04:09:41 pm                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Base Data Access Module"""
from datetime import datetime
from dataclasses import dataclass
import logging
from abc import ABC, abstractmethod
from typing import Dict, List, Tuple
from collections import OrderedDict

from recsys import IMMUTABLE_TYPES, SEQUENCE_TYPES

# ------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #
# ------------------------------------------------------------------------------------------------ #
#                                  SQL COMMAND ABC                                                 #
# ------------------------------------------------------------------------------------------------ #


@dataclass
class SQL(ABC):
    """Base class for SQL Command Objects."""


# ------------------------------------------------------------------------------------------------ #
#                             DDL AGGREGATION BASE CLASS                                           #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class DDL(ABC):
    """Base class for entity DDL."""

    create: SQL
    drop: SQL
    exists: SQL


# ------------------------------------------------------------------------------------------------ #
#                                   SERVICE BASE CLASS                                             #
# ------------------------------------------------------------------------------------------------ #
class Service(ABC):
    def __init__(self) -> None:
        self._logger = logging.getLogger(
            f"{__name__}.{self.__class__.__name__}",
        )


# ------------------------------------------------------------------------------------------------ #
#                              DATA TRANSFER OBJECT ABC                                            #
# ------------------------------------------------------------------------------------------------ #


@dataclass
class DTO(ABC):  # pragma: no cover
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


# ------------------------------------------------------------------------------------------------ #
#                            DATA ACCESS OBJECT ABC                                                #
# ------------------------------------------------------------------------------------------------ #


class DAO(Service):  # pragma: no cover
    def __init__(self) -> None:
        super().__init__()

    @abstractmethod
    def add(self, *args, **kwargs) -> None:
        """Adds an entity to the database."""

    @abstractmethod
    def get(self, id: int) -> DTO:
        """Retrieves an entity from the database, based upon id
        Args:
            id (int): The id for the entity.

        Returns a Data Transfer Object (DTO)
        """

    @abstractmethod
    def get_all(self) -> Dict[str, DTO]:
        """Returns a dictionary of Data Transfer Objects."""

    @abstractmethod
    def update(self, dto: DTO) -> None:
        """Updates an existing entity.

        Args:
            dto (DTO): Data Transfer Object
        """

    @abstractmethod
    def exists(self, id: int) -> bool:
        """Returns True if the entity with id exists in the database.

        Args:
            id (int): id for the entity
        """

    @abstractmethod
    def delete(self, id: int) -> None:
        """Deletes a Dataset from the registry, given an id.
        Args:
            id (int): The id for the entity to delete.
        """

    @abstractmethod
    def _row_to_dto(self, row: Tuple) -> Dict:
        """Converts a row from the database to a DTO object."""

    def _results_to_dict(self, results: List) -> Dict:
        results_dict = OrderedDict()
        for row in results:
            dto = self._row_to_dto(row)
            results_dict[dto.id] = dto
        return results_dict
