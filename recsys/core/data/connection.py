#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/data/connection.py                                                     #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday November 22nd 2022 02:25:42 am                                              #
# Modified   : Friday December 16th 2022 02:50:43 am                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import os
import numpy as np
import sqlite3
from typing import Any
from abc import abstractmethod

from recsys.core.services.base import Service
# ------------------------------------------------------------------------------------------------ #
#                                    CONNECTION                                                    #
# ------------------------------------------------------------------------------------------------ #


class Connection(Service):
    """Abstract base class for DBMS connections."""

    def __init__(self, connector: Any, *args, **kwargs) -> None:
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
