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
# Modified   : Saturday January 21st 2023 04:45:44 am                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2023 John James                                                                 #
# ================================================================================================ #
from abc import ABC, abstractmethod

from dependency_injector.wiring import Provide, inject

from recsys import STATES
from recsys.core.dal.dto import DTO
from recsys.containers import Recsys, EventRepoContainer


# ------------------------------------------------------------------------------------------------ #
#                               PROCESS ABSTRACT BASE CLASS                                        #
# ------------------------------------------------------------------------------------------------ #
class Process(ABC):
    """Base component class from which Task (Leaf) and Job (Composite) objects derive."""

    def __init__(
        self,
        name: str,
        description: str = None,
    ) -> None:
        self._name = name
        self._description = description

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
        """True for Jobs and False for Tasks."""

    # -------------------------------------------------------------------------------------------- #
    @abstractmethod
    def as_dto(self) -> DTO:
        """Creates a dto representation of the process."""

    # -------------------------------------------------------------------------------------------- #
    def _on_create(self) -> None:
        self._state = STATES[0]
        self._callback(self)

    # -------------------------------------------------------------------------------------------- #
    def _on_start(self) -> None:
        self._state = STATES[1]
        self._callback(self)

    # -------------------------------------------------------------------------------------------- #
    def _on_end(self) -> None:
        self._state = STATES[3]
        self._callback(self)

    # -------------------------------------------------------------------------------------------- #
    def _on_fail(self) -> None:
        self._state = STATES[2]
        self._callback(self)


# ------------------------------------------------------------------------------------------------ #
#                            ABSTRACT BASE CLASS FOR CALLBACKS                                     #
# ------------------------------------------------------------------------------------------------ #
class Callback(ABC):
    """Abstract base class used for defining callbacks."""

    @inject
    def __init__(self, events: EventRepoContainer = Provide[Recsys.events]) -> None:
        self._events = events

    @property
    def name(self) -> str:
        """Returns the callback name or lowercase class name if none provided."""
        self._name = self._name or self.__class__.__name__.lower()

    def on_create(self, process: Process) -> None:
        """Called at process (job, task) creation

        May be overriden if this has utility.

        Args:
            process (Process): Process object representation of the process being created.

        """

    @abstractmethod
    def on_start(self, process: Process) -> None:
        """Called when a process (job, task) begins execution.

        Args:
            process (Process): Process object representation of the process which has started.

        """

    @abstractmethod
    def on_fail(self, process: Process) -> None:
        """Called at process (job, task) ends either successfully or otherwise.

        Args:
            process (Process): Process object representation of the process which has ended.

        """

    @abstractmethod
    def on_end(self, process: Process) -> None:
        """Called at process (job, task) ends either successfully or otherwise.

        Args:
            process (Process): Process object representation of the process which has ended.

        """
