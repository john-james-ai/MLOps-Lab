#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/repo/base.py                                                           #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday December 31st 2022 10:47:42 pm                                             #
# Modified   : Wednesday January 11th 2023 07:11:16 pm                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
from abc import ABC, abstractmethod
import logging

from recsys.core.entity.base import Entity


# ------------------------------------------------------------------------------------------------ #
#                                      REPOSITORY ABC                                              #
# ------------------------------------------------------------------------------------------------ #
class RepoABC(ABC):
    """Repository abstract base class"""

    def __init__(self) -> None:
        self._logger = logging.getLogger(
            f"{self.__module__}.{self.__class__.__name__}",
        )

    @abstractmethod
    def __len__(self) -> int:
        """Returns the number of elements in the repository."""

    @abstractmethod
    def add(self, entity: Entity) -> Entity:
        """Adds an entity to the repository and returns the Entity with the id added."""

    @abstractmethod
    def get(self, id: str) -> Entity:
        """Returns an entity with the designated id"""

    @abstractmethod
    def get_by_name(self, name: str) -> Entity:
        """Returns an entity with the given name."""

    @abstractmethod
    def update(self, entity: Entity) -> None:
        """Updates an entity in the database."""

    @abstractmethod
    def remove(self, id: str) -> None:
        """Removes an entity with id from repository."""

    @abstractmethod
    def exists(self, id: str) -> bool:
        """Returns True if entity with id exists in the repository."""

    @abstractmethod
    def print(self) -> None:
        """Prints the repository contents as a DataFrame."""
