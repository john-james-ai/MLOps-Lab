#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/database/sqlite.py                                                     #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday November 22nd 2022 02:25:42 am                                              #
# Modified   : Tuesday December 13th 2022 01:10:28 am                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import os
import sqlite3
import numpy as np
from .base import Connection, Database
# ------------------------------------------------------------------------------------------------ #
#                                    CONNECTION                                                    #
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


# ------------------------------------------------------------------------------------------------ #
#                                       DATABASE                                                   #
# ------------------------------------------------------------------------------------------------ #
class SQLiteDatabase(Database):
    def __init__(self, connection: SQLiteConnection):
        super().__init__(connection=connection)

    def __enter__(self):
        return super().__enter__()

    def __exit__(self, ext_type, exc_value, traceback):
        super().__exit__(ext_type=ext_type, exc_value=exc_value, traceback=traceback)

    def __del__(self):
        super().__del__()

    def _get_last_insert_rowid(self) -> int:
        cursor = self.query(sql="SELECT last_insert_rowid();", args=())
        id = cursor.fetchall()[0][0]
        cursor.close()
        return id
