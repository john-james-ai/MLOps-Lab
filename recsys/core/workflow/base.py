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
# Modified   : Wednesday December 28th 2022 06:25:07 am                                            #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
from abc import abstractmethod
from datetime import datetime
import logging

from recsys import STATES
from recsys.core.repo.uow import UnitOfWork
from recsys.core.entity.base import Entity
from recsys.core.services.validation import Validator


# ------------------------------------------------------------------------------------------------ #
#                                      PROCESS                                                     #
# ------------------------------------------------------------------------------------------------ #
class Process(Entity):
    """Base class for Process classes, such as Job and Task.

    Args:
        name (str): Name for the process.
        description (str): Process description. Defaults <classname>.<name>
    """

    def __init__(self, name: str, description: str = None, uow: UnitOfWork = UnitOfWork(), mode: str = None) -> None:
        super().__init__(name=name, description=description, mode=mode)
        self._uow = uow
        self._validator = Validator()

        self._id = None
        self._description = description or f"{self.__class__.__name__}.{name}"
        self._started = None
        self._ended = None
        self._duration = None
        self._state = STATES[0]

        self._logger = logging.getLogger(
            f"{self.__module__}.{self.__class__.__name__}",
        )

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
    def state(self) -> str:
        return self._state

    @property
    def force(self) -> bool:
        return self._force

    @abstractmethod
    def run(self, data: Entity, uow: UnitOfWork) -> None:
        """Runs the process."""

    def _validate(self) -> None:  # Run at beginning of run method in subclasses.
        self._validator.validate(self)

    def _setup(self) -> None:  # pragma: no cover
        """Executes setup for job."""
        self._started = datetime.now()
        self._state = STATES[2]
        self._modified = datetime.now()
        self._validator.validate(self)

    def _teardown(self) -> None:  # pragma: no cover
        """Completes the job process."""
        self._ended = datetime.now()
        self._duration = (self._ended - self._started).total_seconds()
        self._modified = datetime.now()
        self._state = STATES[-1]
        self._validator.validate(self)
