#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /registry.py                                                                        #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday November 15th 2022 03:53:27 pm                                              #
# Modified   : Thursday November 17th 2022 04:46:30 pm                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Repository registry base module."""
from abc import ABC, abstractmethod
from typing import Any
from recsys.core.services.io import IOService

# ------------------------------------------------------------------------------------------------ #


class Registry(ABC):
    """Repository base class"""

    @property
    @abstractmethod
    def io(self, io: IOService) -> None:
        pass

    @abstractmethod
    def add(self, **kwargs) -> None:
        pass

    @abstractmethod
    def get(self, filename: str) -> Any:
        pass

    @abstractmethod
    def remove(self, filename: str) -> None:
        pass

    @abstractmethod
    def exists(self, filename: str) -> bool:
        pass
