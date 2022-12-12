#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/entity/base.py                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday December 4th 2022 08:30:24 pm                                                #
# Modified   : Sunday December 11th 2022 01:05:21 am                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
from abc import ABC, abstractmethod
from datetime import datetime
import logging

import recsys
from recsys.core.dal.base import DTO

# ------------------------------------------------------------------------------------------------ #


class Entity(ABC):
    """Abstract base class for entity classes.

    It provides several services for the subclasses.
        1. name, datasource, and description are added as private members.
        2. created is initialized to current datetime and private modified member is created.
        3. Properties for id, name, description, and datasource are created.
        4. Validation is provided for name, datasource, workspace, and stage

    It also provides methods to export the Entity as a dictionary.

    """

    def __init__(self, name: str, description: str) -> None:
        self._id = None
        self._name = name
        self._description = description
        self._created = datetime.now()
        self._modified = None
        self._logger = logging.getLogger(
            f"{self.__module__}.{self.__class__.__name__}",
        )

    @property
    def id(self) -> int:
        return self._id

    @id.setter
    def id(self, id: int) -> None:
        if self._id is None:
            self._id = id
            self._modified = datetime.now()
        elif not self._id == id:
            msg = "Item reassignment is not supported for the 'id' member."
            self._logger.error(msg)
            raise TypeError(msg)

    @property
    def name(self) -> str:
        return self._name

    @property
    def description(self) -> str:
        return self._description

    @property
    def created(self) -> str:
        return self._created

    @property
    def modified(self) -> str:
        return self._modified

    @abstractmethod
    def as_dto(self) -> DTO:
        """Returns a Data Transfer Object representation of the entity."""

    @classmethod
    def from_dto(cls, dto: DTO):
        self = cls.__new__(cls)
        self._from_dto(dto)
        return self

    @abstractmethod
    def _from_dto(self, dto: DTO) -> None:
        """Sets the properties and members on the new Entity."""

    def as_dict(self) -> dict:
        """Returns a dictionary representation of the the Config object."""
        return {
            k.replace("_", "", 1) if k[0] == "_" else k: self._export_config(v)
            for k, v in self.__dict__.items()
        }

    @classmethod
    def _export_config(cls, v):
        """Returns v with Configs converted to dicts, recursively."""
        if isinstance(v, recsys.IMMUTABLE_TYPES):
            return v
        elif isinstance(v, recsys.SEQUENCE_TYPES):
            return type(v)(map(cls._export_config, v))
        elif isinstance(v, datetime):
            return v
        elif isinstance(v, dict):
            return v
        elif hasattr(v, "as_dict"):
            return v.as_dict()
        else:
            """Else nothing. What do you want?"""

    def _validate(self) -> None:
        if hasattr(self, "name"):
            if self._name is None:
                msg = f"Error instantiating {self.__class__.__name__}. Attribute 'name' is required for {self.__class__.__name__} objects."
                self._logger.error(msg)
                raise TypeError(msg)

        if hasattr(self, "datasource"):
            if self._datasource is None:
                msg = f"Error instantiating {self.__class__.__name__}. Attribute 'source' is required for {self.__class__.__name__} objects."
                self._logger.error(msg)
                raise TypeError(msg)
            elif self._datasource not in recsys.SOURCES:
                msg = f"Error instantiating {self.__class__.__name__}. Attribute 'source' is invalid. Must be one of {recsys.SOURCES}."
                self._logger.error(msg)
                raise ValueError(msg)

        if hasattr(self, "workspace"):
            if self._workspace is None:
                msg = f"Error instantiating {self.__class__.__name__}. Attribute 'workspace' is required for {self.__class__.__name__} objects."
                self._logger.error(msg)
                raise TypeError(msg)
            elif self._workspace not in recsys.WORKSPACES:
                msg = f"Error instantiating {self.__class__.__name__}. Attribute 'workspace' is invalid. Must be one of {recsys.WORKSPACES}."
                self._logger.error(msg)
                raise ValueError(msg)

        if hasattr(self, "stage"):
            if self._stage is None:
                msg = f"Error instantiating {self.__class__.__name__}. Attribute 'stage' is required for {self.__class__.__name__} objects."
                self._logger.error(msg)
                raise TypeError(msg)
            elif self._stage not in recsys.STAGES:
                msg = f"Error instantiating {self.__class__.__name__}. Attribute 'stage' is invalid. Must be one of {recsys.STAGES}."
                self._logger.error(msg)
                raise ValueError(msg)
