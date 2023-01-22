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
# Modified   : Saturday January 21st 2023 05:58:43 am                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
from abc import ABC, abstractmethod
from datetime import datetime
import logging

import recsys
from recsys.core.service.validation import Validator

# ------------------------------------------------------------------------------------------------ #


class Entity(ABC):
    """Abstract base class for entity classes.

    name (str): Name of entity
    description (str): Optional description of entity

    """

    def __init__(self, name: str, description: str = None) -> None:
        self._name = name
        self._description = description
        self._id = None
        self._oid = self._get_oid()
        self._created = datetime.now()
        self._modified = datetime.now()
        self._validator = Validator()
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
        elif self._id != id:
            msg = "Item re-assignment is not supported for 'id' instance variable."
            self._logger.error(msg)
            raise TypeError(msg)

    @property
    def oid(self) -> str:
        return self._oid

    @property
    def name(self) -> str:
        return self._name

    @property
    def description(self) -> str:
        return self._description

    @abstractmethod
    def as_dto(self):
        """Returns a Data Transfer Object representation of the entity."""

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

    def _get_oid(self) -> str:
        return f"{self.__class__.__name__.lower()}_{self._name}"

    def _validate(self) -> None:
        response = self._validator.validate(self)
        if not response.is_ok:
            self._logger.error(response.msg)
            raise response.exception(response.msg)
