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
# Created    : Saturday January 21st 2023 04:01:43 am                                              #
# Modified   : Saturday January 21st 2023 09:55:00 am                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2023 John James                                                                 #
# ================================================================================================ #
from abc import ABC, abstractmethod
from datetime import datetime
import logging

from dependency_injector.wiring import Provide, inject

from recsys.core.service.validation import Validator
from recsys.core.repo.container import EventRepoContainer
from recsys import IMMUTABLE_TYPES, SEQUENCE_TYPES
from recsys.core.dal.dto import DTO
from recsys import STATES


# ------------------------------------------------------------------------------------------------ #
#                               PROCESS ABSTRACT BASE CLASS                                        #
# ------------------------------------------------------------------------------------------------ #
class Process(ABC):
    """Base component class from which Task (Leaf) and DAG (Composite) objects derive."""

    def __init__(
        self,
        name: str,
        description: str = None,
    ) -> None:
        self._name = name
        self._description = description

        self._oid = self._get_oid()
        self._logger = logging.getLogger(
            f"{self.__module__}.{self.__class__.__name__}",
        )
        self._validator = Validator()
        self._validator.validate()

        self._on_create()

    # -------------------------------------------------------------------------------------------- #
    @property
    def oid(self) -> str:
        return self._oid

    # -------------------------------------------------------------------------------------------- #
    @property
    def name(self) -> str:
        return self._name

    # -------------------------------------------------------------------------------------------- #
    @property
    def description(self) -> str:
        return self._description

    # -------------------------------------------------------------------------------------------- #
    @property
    def state(self) -> str:
        return self._state

    # -------------------------------------------------------------------------------------------- #
    @state.setter
    def state(self, state: str) -> None:
        self._state = state
        self._validate()

    # -------------------------------------------------------------------------------------------- #
    @property
    @abstractmethod
    def is_composite(self) -> str:
        """True for DAGs and False for Tasks."""

    # -------------------------------------------------------------------------------------------- #
    @abstractmethod
    def as_dto(self) -> DTO:
        """Creates a dto representation of the process."""

    # -------------------------------------------------------------------------------------------- #
    def on_create(self) -> None:
        self._state = STATES[0]
        self._callback.on_create(self)

    # -------------------------------------------------------------------------------------------- #
    def on_load(self) -> None:
        self._state = STATES[1]
        self._callback.on_load(self)

    # -------------------------------------------------------------------------------------------- #
    def on_start(self) -> None:
        self._state = STATES[2]
        self._callback.on_start(self)

    # -------------------------------------------------------------------------------------------- #
    def on_fail(self) -> None:
        self._state = STATES[3]
        self._callback.on_fail(self)

    # -------------------------------------------------------------------------------------------- #
    def on_end(self) -> None:
        self._state = STATES[4]
        self._callback.on_end(self)

    # -------------------------------------------------------------------------------------------- #
    def as_dict(self) -> dict:
        """Returns a dictionary representation of the the Config object."""
        return {
            k.replace("_", "", 1) if k[0] == "_" else k: self._export_config(v)
            for k, v in self.__dict__.items()
        }

    @classmethod
    def _export_config(cls, v):
        """Returns v with Configs converted to dicts, recursively."""
        if isinstance(v, IMMUTABLE_TYPES):
            return v
        elif isinstance(v, SEQUENCE_TYPES):
            return type(v)(map(cls._export_config, v))
        elif isinstance(v, datetime):
            return v
        elif isinstance(v, dict):
            return v
        elif hasattr(v, "as_dict"):
            return v.as_dict()
        else:
            """Else nothing. What do you want?"""

    # -------------------------------------------------------------------------------------------- #
    def _get_oid(self) -> str:
        return f"{self.__class__.__name__.lower()}_{self._name}"

    # -------------------------------------------------------------------------------------------- #
    def _validate(self) -> None:
        response = self._validator.validate(self)
        if not response.is_ok:
            self._logger.error(response.msg)
            raise response.exception(response.msg)


# ------------------------------------------------------------------------------------------------ #
#                            ABSTRACT BASE CLASS FOR CALLBACKS                                     #
# ------------------------------------------------------------------------------------------------ #
class Callback(ABC):
    """Abstract base class used for defining callbacks."""

    @inject
    def __init__(self, events: EventRepoContainer = Provide[EventRepoContainer]) -> None:
        self._events = events

    @property
    def name(self) -> str:
        """Returns the callback name or lowercase class name if none provided."""
        self._name = self._name or self.__class__.__name__.lower()

    def on_create(self, process: Process) -> None:
        """Called at process (dag, task) creation

        May be overriden if this has utility.

        Args:
            process (Process): Process object representation of the process being created.

        """

    @abstractmethod
    def on_start(self, process: Process) -> None:
        """Called when a process (dag, task) begins execution.

        Args:
            process (Process): Process object representation of the process which has started.

        """

    @abstractmethod
    def on_fail(self, process: Process) -> None:
        """Called at process (dag, task) ends either successfully or otherwise.

        Args:
            process (Process): Process object representation of the process which has ended.

        """

    @abstractmethod
    def on_end(self, process: Process) -> None:
        """Called at process (dag, task) ends either successfully or otherwise.

        Args:
            process (Process): Process object representation of the process which has ended.

        """
