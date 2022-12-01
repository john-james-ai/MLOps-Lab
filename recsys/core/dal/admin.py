#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /admin.py                                                                           #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday December 1st 2022 06:29:25 am                                              #
# Modified   : Thursday December 1st 2022 10:42:07 am                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Database Administration Module."""
from abc import ABC, abstractmethod

from ..data.database import SqliteDatabase
from .sequel import Sequel


# ------------------------------------------------------------------------------------------------ #


class DBAdmin(ABC):
    def __init__(self, database: SqliteDatabase) -> None:
        self._database = database

    @abstractmethod
    def create_table(self, sequel: Sequel) -> None:
        """Creates a table if not exists in an existing database."""

    @abstractmethod
    def drop_table(self, sequel: Sequel) -> None:
        """Drops a table from an existing database, if it exists."""

    @abstractmethod
    def table_exists(self, sequel: Sequel) -> bool:
        """Returns true if the table exists."""


# ------------------------------------------------------------------------------------------------ #


class SQLiteAdmin(DBAdmin):
    def __init__(self, database: SqliteDatabase) -> None:
        self._database = database

    def create_table(self, sequel: Sequel) -> None:
        """Creates a table if not exists in an existing database."""
        with self._database as db:
            db.create_table(sequel.sql, sequel.args)

    def drop_table(self, sequel: Sequel) -> None:
        """Drops a table from an existing database, if it exists."""
        with self._database as db:
            db.drop_table(sequel.sql, sequel.args)

    def table_exists(self, sequel: Sequel) -> bool:
        """Returns true if the table exists."""
        with self._database as db:
            db.exists(sequel.sql, sequel.args)
