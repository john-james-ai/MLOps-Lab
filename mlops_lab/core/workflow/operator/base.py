#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Enter Project Name in Workspace Settings                                            #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /mlops_lab/core/workflow/operator/base.py                                           #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : Enter URL in Workspace Settings                                                     #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday December 4th 2022 08:30:24 pm                                                #
# Modified   : Tuesday January 24th 2023 08:13:47 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
from abc import ABC, abstractmethod
import logging
from typing import Any

from mlops_lab.core.repo.uow import UnitOfWork


# ================================================================================================ #
#                                    OPERATOR BASE CLASS                                           #
# ================================================================================================ #
class Operator(ABC):
    """Operator Base Class"""

    def __init__(self, *args, **kwargs) -> None:
        self._logger = logging.getLogger(
            f"{self.__module__}.{self.__class__.__name__}",
        )

    def __str__(self) -> str:
        return f"Operator:\n\tModule: {self.__module__}\n\tClass: {self.__class__.__name__}"

    def __repr__(self) -> str:
        return f"{self.__module__}, {self.__class__.__name__}"

    @abstractmethod
    def execute(self, uow: UnitOfWork, data: Any = None) -> None:
        """Executes the operation."""
