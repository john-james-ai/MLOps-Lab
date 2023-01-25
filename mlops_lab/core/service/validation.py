#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Enter Project Name in Workspace Settings                                            #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /mlops_lab/core/service/validation.py                                               #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : Enter URL in Workspace Settings                                                     #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday December 27th 2022 02:41:20 pm                                              #
# Modified   : Tuesday January 24th 2023 08:13:44 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Validation Module."""
from abc import ABC, abstractmethod
import logging

from mlops_lab.core.entity.dataset import STAGES
from mlops_lab.core.workflow import STATES


# ------------------------------------------------------------------------------------------------ #
#                                    VALIDATOR ABC DESCRIPTOR                                      #
# ------------------------------------------------------------------------------------------------ #
class Validator(ABC):
    """Abstract base class for validation descriptors."""

    def __set_name__(self, owner, name):
        self.property_name = name
        self.private_name = "_" + name
        self._logger = logging.getLogger(
            f"{owner.__module__}.{owner.__class__.__name__}",
        )

    def __get__(self, obj, objtype=None):
        return getattr(obj, self.private_name)

    def __set__(self, obj, value):
        self.validate(value)
        setattr(obj, self.private_name, value)

    @abstractmethod
    def validate(self, value):
        pass


# ------------------------------------------------------------------------------------------------ #
#                                   STAGE VALIDATOR                                                #
# ------------------------------------------------------------------------------------------------ #
class ValidStage(Validator):
    """Validates the data processing stage for datasets."""

    def validate(self, value) -> None:
        if value is None:
            msg = f"The {self.property_name!r} must not be None."
            self._logger.error(msg)
            raise ValueError(msg)

        if not isinstance(value, str):
            msg = f"The {self.property_name!r} must a string."
            self._logger.error(msg)
            raise TypeError(msg)

        if value not in STAGES:
            msg = f"Expected {value!r} to be one of {STAGES!r}"
            self._logger.error(msg)
            raise ValueError(msg)


# ------------------------------------------------------------------------------------------------ #
#                                   STATE VALIDATOR                                                #
# ------------------------------------------------------------------------------------------------ #
class ValidState(Validator):
    """Validates the value of state for Process and Event objects."""

    def validate(self, value) -> None:
        if value is None:
            msg = f"The {self.property_name!r} must not be None."
            self._logger.error(msg)
            raise ValueError(msg)

        if not isinstance(value, str):
            msg = f"The {self.property_name!r} must a string."
            self._logger.error(msg)
            raise TypeError(msg)

        if value not in STATES:
            msg = f"Expected {value!r} to be one of {STATES!r}"
            self._logger.error(msg)
            raise ValueError(msg)
