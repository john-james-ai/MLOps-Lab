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
# Modified   : Thursday November 17th 2022 04:50:13 pm                                             #
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


# ------------------------------------------------------------------------------------------------ #


@dataclass
class RepoConfig(Config):
    name: str = "repo_config"


# ------------------------------------------------------------------------------------------------ #


class RepoBuilder(ABC):
    """Repository builder base class."""

    @property
    def repo(self) -> Repo:
        pass

    @abstractmethod
    def build_config(self, config: RepoConfig) -> None:
        pass

    @abstractmethod
    def build_registry(self) -> None:
        pass

    @abstractmethod
    def build_repo(self) -> None:
        pass


# ------------------------------------------------------------------------------------------------ #


class RepoDirector(ABC):
    """Repository director responsible for executing the steps of the RepoBuilder in a sequence.

    Args:
        config_filepath (str): The path to the builder configuration file
        builder (RepoBuilder): The concrete builder class
    """

    def __init__(self, config: RepoConfig, builder: RepoBuilder) -> None:
        self._config = config
        self._builder = builder

    @property
    def builder(self) -> RepoBuilder:
        return self._builder

    @builder.setter
    def builder(self, builder: RepoBuilder) -> None:
        self._builder = builder
