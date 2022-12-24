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
# Modified   : Saturday December 24th 2022 06:16:05 am                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
from abc import abstractmethod
from datetime import datetime
import logging

import recsys
from recsys.core.entity.base import Entity


# ------------------------------------------------------------------------------------------------ #
#                                      PROCESS                                                     #
# ------------------------------------------------------------------------------------------------ #
class Process(Entity):
    """Base class for Process classes, such as Job and Task."""

    def __init__(self) -> None:
        self._id = None
        self._name = None
        self._description = None
        self._mode = None
        self._started = None
        self._ended = None
        self._duration = None
        self._state = recsys.STATES[0]
        self._created = datetime.now()
        self._modified = None
        self._force = None
        self._logger = logging.getLogger(
            f"{self.__module__}.{self.__class__.__name__}",
        )

    @property
    def name(self) -> str:
        return self._name

    @name.setter
    def name(self, name: str) -> None:
        self._name = name
        self._modified = datetime.now()

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

    @mode.setter
    def mode(self, mode: str) -> None:
        self._mode = mode
        self._modified = datetime.now()

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
    def force(self) -> bool:
        return self._force

    @force.setter
    def force(self, force: str) -> None:
        self._force = force
        self._modified = datetime.now()

    @abstractmethod
    def run(self) -> None:
        """Runs the process."""

    def _validate(self) -> None:  # Run at beginning of run method in subclasses.
        super()._validate()
        if hasattr(self, "state"):
            if self._state is None:
                msg = f"Error instantiating {self.__class__.__name__}. Attribute 'state' is required for {self.__class__.__name__} objects."
                self._logger.error(msg)
                raise TypeError(msg)
            elif self._state not in recsys.STATES:
                msg = f"Error instantiating {self.__class__.__name__}. Attribute 'state' is invalid. Must be one of {recsys.STATES}."
                self._logger.error(msg)
                raise ValueError(msg)

        self._state = recsys.STATES[1]

    def _setup(self) -> None:  # pragma: no cover
        """Executes setup for job."""
        self._started = datetime.now()
        self._validate()
        self._state = recsys.STATES[2]
        self._modified = datetime.now()

    def _teardown(self) -> None:  # pragma: no cover
        """Completes the job process."""
        self._ended = datetime.now()
        self._duration = (self._ended - self._started).total_seconds()
        self._state = recsys.STATES[-1]
        self._modified = datetime.now()
