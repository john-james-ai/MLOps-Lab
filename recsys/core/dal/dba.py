#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/dal/dba.py                                                             #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday December 4th 2022 06:27:36 am                                                #
# Modified   : Wednesday January 4th 2023 04:51:17 am                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Data Definition Object Module."""
from abc import ABC, abstractmethod
import logging

from recsys.core.database.relational import Database
from recsys.core.dal.base import DDL


# ------------------------------------------------------------------------------------------------ #
#                                    ABSTRACT DBA                                                  #
# ------------------------------------------------------------------------------------------------ #
class AbstractDBA(ABC):
    "Abstract base class for Data Base Administration objects."

    def __init__(self, ddl: DDL, autocommit: bool = True) -> None:
        self._ddl = ddl
        self._autocommit = autocommit
        self._logger = logging.getLogger(
            f"{self.__module__}.{self.__class__.__name__}",
        )
        self._database = None

    @property
    def database(self) -> Database:
        return self._database

    @database.setter
    def database(self, database: Database) -> None:
        self._database = database

    @abstractmethod
    def create(self) -> None:
        """Creates a database, object store or table."""

    @abstractmethod
    def drop(self) -> None:
        """Drops a database, object store or table."""

    @abstractmethod
    def exists(self) -> bool:
        """Returns True if the object exists, False otherwise."""


# ------------------------------------------------------------------------------------------------ #
#                                           DBA                                                    #
# ------------------------------------------------------------------------------------------------ #
class DBA(AbstractDBA):
    """Supports basic database and table administration.."""
    def __init__(self, ddl: DDL, autocommit: bool = True) -> None:
        super().__init__(ddl=ddl, autocommit=autocommit)

    def create(self) -> None:
        """Creates a database."""
        self._database.create(sql=self._ddl.create.sql, args=self._ddl.create.args)
        if self._autocommit:
            self._database.save()
        msg = self._ddl.create.description
        self._logger.info(msg)

    def drop(self) -> None:
        """Drops a database or table."""
        self._database.drop(sql=self._ddl.drop.sql, args=self._ddl.drop.args)
        if self._autocommit:
            self._database.save()
        msg = self._ddl.drop.description
        self._logger.info(msg)

    def exists(self) -> None:
        """Checks existence of a database."""
        msg = self._ddl.exists.description
        self._logger.info(msg)
        return self._database.exists(sql=self._ddl.exists.sql, args=self._ddl.exists.args)

    def reset(self) -> None:
        self.drop()
        self._database.save()
        self.create()
        self._database.save()
