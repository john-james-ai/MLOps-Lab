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
# Modified   : Monday January 9th 2023 05:28:56 pm                                                 #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Data Definition Object Module."""
from abc import ABC, abstractmethod
import logging

from recsys.core.database.relational import Database
from recsys.core.database.object import ObjectDB
from recsys.core.dal.sql.base import DDL, ODL


# ------------------------------------------------------------------------------------------------ #
#                                    ABSTRACT DBA                                                  #
# ------------------------------------------------------------------------------------------------ #
class AbstractDBA(ABC):
    "Abstract base class for Data Base Administration objects."

    def __init__(self, ddl: DDL, database: Database) -> None:
        self._ddl = ddl
        self._database = database
        self._logger = logging.getLogger(
            f"{self.__module__}.{self.__class__.__name__}",
        )

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
#                                          TABLE ADMIN                                             #
# ------------------------------------------------------------------------------------------------ #
class DBA(AbstractDBA):
    """Supports basic database and table administration.."""
    def __init__(self, ddl: DDL, database: Database) -> None:
        super().__init__(ddl=ddl, database=database)

    def create(self) -> None:
        """Creates a database."""
        self._database.connect()

        self._database.create(sql=self._ddl.create.sql, args=self._ddl.create.args)
        msg = self._ddl.create.description
        self._logger.info(msg)

        self._database.save()
        self._database.close()

    def drop(self) -> None:
        """Drops a database or table."""
        self._database.connect()

        self._database.drop(sql=self._ddl.drop.sql, args=self._ddl.drop.args)
        msg = self._ddl.drop.description
        self._logger.info(msg)

        self._database.save()
        self._database.close()

    def exists(self) -> None:
        """Checks existence of a database."""
        self._database.connect()

        result = self._database.exists(sql=self._ddl.exists.sql, args=self._ddl.exists.args)
        msg = self._ddl.exists.description
        self._logger.info(msg)

        self._database.close()

        return result

    def reset(self) -> None:
        self.drop()
        self.create()


# ------------------------------------------------------------------------------------------------ #
#                                       OBJECT DB ADMIN                                            #
# ------------------------------------------------------------------------------------------------ #
class ODBA(AbstractDBA):
    """Supports object database definition."""
    def __init__(self, ddl: ODL, database: ObjectDB) -> None:
        super().__init__(ddl=ddl, database=database)

    def create(self) -> None:
        """Creates an object database."""
        raise NotImplementedError()

    def drop(self) -> None:
        """Drops a database or table."""
        self._database.drop()

    def exists(self) -> None:
        """Checks existence of a database."""
        return self._database.exists()

    def reset(self) -> None:
        self.drop()
