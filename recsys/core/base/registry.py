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
# Modified   : Sunday November 20th 2022 10:14:23 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Repository registry base module."""
from abc import ABC, abstractclassmethod
from typing import Any

# ------------------------------------------------------------------------------------------------ #


class Registry(ABC):
    """Repository base class"""

    @property
    @abstractclassmethod
    def count(clf) -> int:
        pass

    @abstractclassmethod
    def add(clf, **kwargs) -> None:
        pass

    @abstractclassmethod
    def get(clf, filename: str) -> Any:
        pass

    @abstractclassmethod
    def remove(clf, filename: str) -> None:
        pass

    @abstractclassmethod
    def exists(clf, filename: str) -> bool:
        pass
