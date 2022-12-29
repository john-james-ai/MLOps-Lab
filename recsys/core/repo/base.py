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
# Created    : Friday December 16th 2022 12:28:07 am                                               #
# Modified   : Wednesday December 28th 2022 03:02:53 pm                                            #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import os
from dataclasses import dataclass
import dotenv
from abc import ABC, abstractmethod
import logging

from dependency_injector.wiring import Provide

from recsys.core.dal.dao import DAO
from recsys.core.entity.base import Entity
from recsys.containers import Recsys


# ------------------------------------------------------------------------------------------------ #
#                                        CONTEXT                                                   #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class Context:

    dataset: DAO = Provide[Recsys.dao.dataset]
    dataframe: DAO = Provide[Recsys.dao.dataframe]
    task: DAO = Provide[Recsys.dao.task]
    job: DAO = Provide[Recsys.dao.job]
    profile: DAO = Provide[Recsys.dao.profile]

    @classmethod
    def save(cls) -> None:
        cls.dataset.save()
        cls.dataframe.save()
        cls.task.save()
        cls.job.save()
        cls.profile.save()


# ------------------------------------------------------------------------------------------------ #
#                                       REPOSITORY                                                 #
# ------------------------------------------------------------------------------------------------ #
class Repo(ABC):
    """Repository base class"""

    def __init__(self, context: Context) -> None:
        self._context = context
        self._logger = logging.getLogger(
            f"{self.__module__}.{self.__class__.__name__}",
        )

    @abstractmethod
    def __len__(self) -> int:
        """Returns the number of aggregate root elements in the repository."""

    @abstractmethod
    def add(self, entity: Entity) -> Entity:
        """Adds an entity to the repository and returns the Entity with the id added."""

    @abstractmethod
    def get(self, id: str) -> Entity:
        """Returns an entity with the designated id"""

    @abstractmethod
    def get_by_name_mode(self, name: str) -> Entity:
        """Returns an entity with the given name."""

    @abstractmethod
    def get_all(self) -> dict:
        """Returns all aggregate root entities in the repository."""

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

    def _get_mode(self) -> str:
        dotenv.load_dotenv()
        return os.getenv("MODE")
