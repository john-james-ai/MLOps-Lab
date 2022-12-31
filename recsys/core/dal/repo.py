#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/dal/repo.py                                                            #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday December 16th 2022 12:28:07 am                                               #
# Modified   : Friday December 30th 2022 08:50:15 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import os
import dotenv
from abc import ABC, abstractmethod
from dataclasses import dataclass
import logging

from dependency_injector.wiring import Provide
from recsys.core.dal.dao import DAO
from recsys.core.entity.base import Entity
from recsys.core.entity.dataset import Dataset, DataFrame
from recsys.core.entity.datasource import DataSource, DataSourceURL
from recsys.core.entity.job import Task, Job
from recsys.core.entity.file import File
from recsys.core.entity.profile import Profile

from recsys.containers import Recsys


# ------------------------------------------------------------------------------------------------ #
#                                        CONTEXT                                                   #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class Context:

    file: DAO = Provide[Recsys.dao.file]
    datasource: DAO = Provide[Recsys.dao.datasource]
    datasource_url: DAO = Provide[Recsys.dao.datasource_url]
    dataset: DAO = Provide[Recsys.dao.dataset]
    dataframe: DAO = Provide[Recsys.dao.dataframe]
    task: DAO = Provide[Recsys.dao.task]
    job: DAO = Provide[Recsys.dao.job]
    profile: DAO = Provide[Recsys.dao.profile]

    @classmethod
    def get_dao(cls, entity: type(Entity)) -> DAO:

        cls.logger = logging.getLogger(
            f"{cls.__module__}.{cls.__class__.__name__}",
        )

        daos = {Dataset: cls.dataset, DataFrame: cls.dataframe, DataSource: cls.datasource,
                DataSourceURL: cls.datasource_url, Task: cls.task, Job: cls.job,
                Profile: cls.profile, File: cls.file}
        try:
            return daos[entity]
        except KeyError:
            msg = f'Error: {entity} does not have a data access object (DAO).'
            cls.logger.error(msg)
            raise ValueError(msg)

    @classmethod
    def save(cls) -> None:
        cls.file.save()
        cls.datasource.save()
        cls.datasource_url.save()
        cls.dataset.save()
        cls.dataframe.save()
        cls.task.save()
        cls.job.save()
        cls.profile.save()


# ------------------------------------------------------------------------------------------------ #
#                                      REPOSITORY ABC                                              #
# ------------------------------------------------------------------------------------------------ #
class RepoABC(ABC):
    """Repository abstract base class"""

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


# ------------------------------------------------------------------------------------------------ #
#                                           REPOSITORY                                             #
# ------------------------------------------------------------------------------------------------ #
class Repo(RepoABC):
    """Repository base class"""

    def __init__(self, context: Context, entity: type(Entity)) -> None:
        self._context = context
        self._entity = entity
        self._dao = self._context.get_dao(self._entity)
        self._logger = logging.getLogger(
            f"{self.__module__}.{self.__class__.__name__}",
        )

    def __len__(self) -> int:
        return len(self.get_all())

    def add(self, entity: Entity) -> Entity:
        """Adds an entity to the repository and returns the Entity with the id added."""
        entity = self._dao.create(entity)
        return entity

    def get(self, id: str) -> Entity:
        "Returns an entity with the designated id"
        return self._dao.read(id)

    def get_by_name_mode(self, name: str, mode: str = None) -> Entity:
        mode = mode or self._get_mode()
        return self._dao.read_by_name_mode(name, mode)

    def get_all(self) -> dict:
        return self._dao.read_all()

    def update(self, entity: Entity) -> None:
        """Updates an entity in the database."""
        self._dao.update(entity)

    def remove(self, id: str) -> None:
        """Removes an entity (and its children) from repository."""
        self._dao.delete(id)

    def exists(self, id: str) -> bool:
        """Returns True if entity with id exists in the repository."""
        return self._dao.exists(id)

    def print(self) -> None:
        """Prints the repository contents as a DataFrame."""
        entities = self.get_all()
        for id, entity in entities.items():
            print("\n")
            print(entity)
