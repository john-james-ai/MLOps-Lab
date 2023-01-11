#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/repo/uow.py                                                            #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday December 25th 2022 12:55:35 pm                                               #
# Modified   : Tuesday January 10th 2023 07:25:54 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Unit of Work Module"""
from abc import ABC, abstractmethod

from recsys.core.entity.base import Entity
from recsys.core.repo.context import Context
from recsys.core.repo.entity import Repo


# ------------------------------------------------------------------------------------------------ #
#                                 UNIT OF WORK ABC                                                 #
# ------------------------------------------------------------------------------------------------ #
class UnitOfWorkABC(ABC):  # pragma: no cover

    def __init__(self, context: Context) -> None:
        self._context = context

    @abstractmethod
    def register(self, name: str, repo: type(Repo), entity: type(Entity) = None) -> None:
        """Returns an instantiated datasource repository."""

    @abstractmethod
    def get_repo(self, name) -> Repo:
        """Returns an instantiated file repository."""

    @abstractmethod
    def save(self):
        """Save changes."""

    @abstractmethod
    def rollback(self):
        """Rolls back changes since last save."""


# ------------------------------------------------------------------------------------------------ #
#                                     UNIT OF WORK ABC                                             #
# ------------------------------------------------------------------------------------------------ #
class UnitOfWork(UnitOfWorkABC):
    """Unit of Work object containing all Entity repositories and the current context entity.

    Args:
        context (Context): Contains the database context in terms of Database Access Objects.

    """

    def __init__(self, context: Context) -> None:
        self._context = context
        self._in_transaction = False
        self._repos = {}

    def register(self, name: str, repo: type(Repo), entity: type(Entity) = None) -> None:
        self._begin()
        self._repos[name] = repo(context=self._context, entity=entity)

    def get_repo(self, name) -> Repo:
        return self._repos[name]

    def save(self) -> None:
        self._context.save()

    def rollback(self) -> None:
        self._context.rollback()

    def _begin(self) -> None:
        """Begins a transaction if not already in one."""
        if not self._in_transaction:
            self._context.begin()
