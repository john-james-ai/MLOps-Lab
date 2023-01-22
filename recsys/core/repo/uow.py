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
# Modified   : Saturday January 21st 2023 09:14:34 am                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Unit of Work Module"""
from abc import ABC, abstractmethod
import logging

from dependency_injector import providers

from recsys.core.repo.entity import Repo


# ------------------------------------------------------------------------------------------------ #
#                                 UNIT OF WORK ABC                                                 #
# ------------------------------------------------------------------------------------------------ #
class UnitOfWorkABC(ABC):  # pragma: no cover
    def __init__(self) -> None:
        self._logger = logging.getLogger(
            f"{self.__module__}.{self.__class__.__name__}",
        )

    @abstractmethod
    def get_repo(self, name) -> Repo:
        """Returns an instantiated file repository."""

    @abstractmethod
    def begin(self):
        """Starts a transaction."""

    @abstractmethod
    def save(self):
        """Save changes."""

    @abstractmethod
    def rollback(self):
        """Rolls back changes since last save."""

    @abstractmethod
    def close(self):
        """Closes the unit of work."""


# ------------------------------------------------------------------------------------------------ #
#                                     UNIT OF WORK ABC                                             #
# ------------------------------------------------------------------------------------------------ #
class UnitOfWork(UnitOfWorkABC):
    """Unit of Work object containing all Entity repositories and the current context entity.

    A transaction is started when the unit of work is instantiated. Transactions
    are extant until an explicit save or rollback is called. Begin starts a transaction only if
    the prior transaction has been closed or garbage collected. Nested transactions are not supported.

    Args:
        entities (providers.Container): A container of entity repositories that share a common context.

    Release Notes:
    1. dag, event, and profile repositories were removed as they contain non-entities which
        are subject to a different persistence and lifetime regimes. Entity repositories
        are included for transaction isolation. Events, dags, and dag profiles are not
        transaction isolated.

    """

    def __init__(self, entities: providers.Container) -> None:
        super().__init__()
        self._context = entities.context()
        self._repos = {}
        self._repos["file"] = entities.file()
        self._repos["datasource"] = entities.datasource()
        self._repos["dataset"] = entities.dataset()
        self._in_transaction = False
        self._dag = None
        msg = f"Instantiated UoW at {id(self)}."
        self._logger.debug(msg)

    def __enter__(self):
        """Start a transaction"""
        self.begin()
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        if exc_type:
            self.rollback()
            self._logger.error(f"Exception Type: {exc_type}")
            self._logger.error(f"Exception Value: {exc_value}")
            self._logger.error(f"Exception Traceback: {exc_tb}")
        else:
            self.save()
            self.close()

    def get_repo(self, name) -> Repo:
        return self._repos[name]

    def begin(self) -> None:
        if not self._in_transaction:
            self._context.begin()
            self._in_transaction = True

    def save(self) -> None:
        self._context.save()
        self._in_transaction = False

    def rollback(self) -> None:
        self._context.rollback()
        self._in_transaction = False

    def close(self) -> None:
        self._context.close()
