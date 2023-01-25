#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Enter Project Name in Workspace Settings                                            #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /mlops_lab/core/factory/base.py                                                     #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : Enter URL in Workspace Settings                                                     #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday December 19th 2022 03:34:43 pm                                               #
# Modified   : Tuesday January 24th 2023 08:13:41 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""DAG Module"""
from abc import ABC, abstractmethod
import logging

from mlops_lab.core.entity.base import Entity


# ------------------------------------------------------------------------------------------------ #
#                                        FACTORY                                                   #
# ------------------------------------------------------------------------------------------------ #
class Factory(ABC):
    """Abstact Base Class for Factory classes. Not an Abstact Factory."""

    def __init__(self) -> None:
        self._instance = None
        self._logger = logging.getLogger(
            f"{self.__module__}.{self.__class__.__name__}",
        )

    @abstractmethod
    def __call__(self, config: dict) -> Entity:
        """Returns an instance of the requested entity"""
