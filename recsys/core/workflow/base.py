#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/workflow/base.py                                                       #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday December 4th 2022 08:30:24 pm                                                #
# Modified   : Tuesday December 20th 2022 05:36:40 pm                                              #
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
STATES = ['CREATED', 'READY', 'IN-PROGRESS', 'FAILED', 'COMPLETE']


# ------------------------------------------------------------------------------------------------ #
class Process(ABC):
    """Abstract base class for Process classes"""

    def __init__(self) -> None:
        self._id = None
        self._name = None
        self._description = None
        self._mode = None
        self._started = None
        self._ended = None
        self._duration = None
        self._state = STATES[0]
        self._created = datetime.now()
        self._modified = None
        self._force = None
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
        else:
            msg = "Item reassignment is not supported for the 'id' member."
            self._logger.error(msg)
            raise TypeError(msg)

    @property
    def name(self) -> str:
        return self._name

    @name.setter
    def name(self, name: str) -> None:
        if self._name is None:
            self._name = name
            self._modified = datetime.now()
        else:
            msg = "Item reassignment is not supported for the 'name' member."
            self._logger.error(msg)
            raise TypeError(msg)

    @property
    def description(self) -> str:
        return self._description

    @description.setter
    def description(self, description: str) -> None:
        self._description = description
        self._modified = datetime.now()

    @property
    def mode(self) -> str:
        return self._mode

    @property
    def started(self) -> str:
        return self._started

    @property
    def ended(self) -> str:
        return self._ended

    @property
    def duration(self) -> str:
        return self._duration

    @property
    def created(self) -> str:
        return self._created

    @property
    def modified(self) -> str:
        return self._modified

    @property
    def force(self) -> bool:
        return self._force

    @force.setter
    def force(self, force: str) -> None:
        self._force = force
        self._modified = datetime.now()

    @property
    def state(self) -> str:
        return self._state

    @state.setter
    def state(self, state: str) -> None:
        self._state = state
        self._modified = datetime.now()

    @abstractmethod
    def run(self) -> None:
        """Runs the process."""

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

    def _validate(self) -> None:  # Run at beginning of run method in subclasses.
        if hasattr(self, "name"):
            if self._name is None:
                msg = f"Error instantiating {self.__class__.__name__}. Attribute 'name' is required for {self.__class__.__name__} objects."
                self._logger.error(msg)
                raise TypeError(msg)

        if hasattr(self, "mode"):
            if self._mode is None:
                msg = f"Error instantiating {self.__class__.__name__}. Attribute 'mode' is required for {self.__class__.__name__} objects."
                self._logger.error(msg)
                raise TypeError(msg)
            elif self._mode not in recsys.MODES:
                msg = f"Error instantiating {self.__class__.__name__}. Attribute 'mode' is invalid. Must be one of {recsys.MODES}."
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

    def _setup(self) -> None:  # pragma: no cover
        """Executes setup for job."""
        self._started = datetime.now()
        self._validate()
        self._state = STATES[2]

    def _teardown(self) -> None:  # pragma: no cover
        """Completes the job process."""
        self._ended = datetime.now()
        self._duration = (self._ended - self._started).total_seconds()
        self._state = STATES[-1]
