#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /base.py                                                                            #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday December 1st 2022 05:44:55 am                                              #
# Modified   : Thursday December 1st 2022 05:45:26 am                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Base Repository Module"""
import logging
from abc import ABC, abstractmethod
from typing import Any

# ------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #


class Repo(ABC):
    """Repository base class"""

    @abstractmethod
    def add(self, *args, **kwargs) -> None:  # pragma: no cover
        pass

    @abstractmethod
    def get(self, id: str) -> Any:  # pragma: no cover
        pass

    @abstractmethod
    def remove(self, id: str) -> None:  # pragma: no cover
        pass

    @abstractmethod
    def exists(self, id: str) -> bool:  # pragma: no cover
        pass
