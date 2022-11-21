#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /repo.py                                                                            #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday November 13th 2022 09:55:54 pm                                               #
# Modified   : Sunday November 20th 2022 09:07:33 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Base Module for the (Data) Repository and the Datasets it contains."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

from recsys.core.base.registry import Registry
from recsys.core.base.config import Config

# ------------------------------------------------------------------------------------------------ #


class Repo(ABC):
    """Repository base class"""

    def __init__(self) -> None:
        self._registry = None

    @property
    def registry(self) -> Registry:
        return self._registry

    @registry.setter
    def registry(self, registry: Registry) -> None:
        self._registry = registry

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
