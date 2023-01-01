#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/database/connection.py                                                 #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday December 24th 2022 12:55:33 pm                                             #
# Modified   : Saturday December 31st 2022 08:43:14 pm                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import os
from glob import glob
from typing import Any
import numpy as np
import sqlite3
from abc import abstractmethod
import shelve

from recsys.core.services.base import Service
# ------------------------------------------------------------------------------------------------ #
#                                    CONNECTION                                                    #
# ------------------------------------------------------------------------------------------------ #


class Connection(Service):
    """Abstract base class for DBMS connections."""

    def __init__(self, connector: Any = None, *args, **kwargs) -> None:
        super().__init__()
        self._connector = connector
        self._connection = None
        self.connect()

    @abstractmethod
    def connect(self) -> Any:
        """Connects to the underlying database"""

    def is_connected(self) -> bool:
        try:
            self._connection.cursor()
            return True
        except Exception:
            return False

    def close(self) -> None:
        """Closes the connection."""
        self._connection.close()
        self._logger.debug(f"{self.__class__.__name__}.connection is closed.")

    def commit(self) -> None:
        """Commits the connection"""
        self._connection.commit()
        self._logger.debug(f"{self.__class__.__name__}.connection is committed.")

    def cursor(self) -> None:
        """Returns a the connection cursor"""
        return self._connection.cursor()

    def rollback(self) -> None:
        """Rolls back the database to the last commit."""
        self._connection.rollback()
        self._logger.debug(f"{self.__class__.__name__}.connection is rolled back.")

# ------------------------------------------------------------------------------------------------ #
#                                    SQLITE CONNECTION                                             #
# ------------------------------------------------------------------------------------------------ #


class SQLiteConnection(Connection):
    """Connection to the underlying SQLite DBMS."""

    def __init__(self, connector: sqlite3.connect, location: str) -> None:
        self._location = location
        sqlite3.register_adapter(np.int64, lambda val: int(val))
        sqlite3.register_adapter(np.int32, lambda val: int(val))
        os.makedirs(os.path.dirname(self._location), exist_ok=True)
        super().__init__(connector=connector)

    def connect(self) -> sqlite3.Connection:
        self._connection = self._connector(
            self._location, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES
        )
        self._logger.debug(f"{self.__class__.__name__}.connection is connected.")


# ------------------------------------------------------------------------------------------------ #
#                             OBJECT DATABASE (PSEUDO) CONNECTION                                  #
# ------------------------------------------------------------------------------------------------ #
class ODBConnection(Service):

    def __init__(self, dbfile: str) -> None:
        super().__init__()
        self._dbfile = dbfile
        self._cache = {}
        self._connection = None
        self._is_connected = False
        os.makedirs(os.path.dirname(self._dbfile), exist_ok=True)

    @property
    def cache(self) -> dict:
        return self._cache

    @cache.setter
    def cache(self, cache: dict) -> None:
        self._cache = cache

    def reset(self) -> None:
        self.close()
        self._delete_dbfiles()
        self._reset_cache()
        self.connect()
        self._logger.debug(f"Reset object database at {self._dbfile}")

    def connect(self) -> shelve.Shelf:
        self._connection = shelve.open(self._dbfile)
        self._is_connected = True
        self._logger.debug("ODBConnection is open")

    def is_connected(self) -> bool:
        return self._is_connected

    def cursor(self) -> shelve.Shelf:
        return self._connection

    def close(self) -> None:
        if self._is_connected:
            self._connection.close()
        self._is_connected = False
        self._logger.debug("ODBConnection is closed")

    def commit(self) -> None:
        if not self.is_connected():
            self.connect()
        for k, v in self._cache.items():
            if v is not None:
                self._connection[k] = v
            else:
                del self._connection[k]
        self._cache = {}
        self._logger.debug("ODBConnection is committed")

    def rollback(self) -> None:
        self._cache = {}
        self._logger.debug("ODBConnection is rolled back")

    def _delete_dbfiles(self) -> None:
        dbfiles = self._dbfile + ".*"
        for f in glob(dbfiles):
            os.remove(f)

    def _reset_cache(self) -> None:
        self._cache = {}
