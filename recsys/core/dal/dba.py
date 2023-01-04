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
# Modified   : Monday January 2nd 2023 07:40:38 pm                                                 #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Data Definition Object Module."""

from recsys.core.services.base import Service
from recsys.core.database.relational import Database
from recsys.core.database.object import ObjectDB
from recsys.core.dal.base import DDL
# ------------------------------------------------------------------------------------------------ #

# ------------------------------------------------------------------------------------------------ #
#                                       TABLE SERVICE                                              #
# ------------------------------------------------------------------------------------------------ #


class DBA(Service):
    def __init__(self, database: Database, object_db: ObjectDB, ddl: DDL) -> None:
        self._database = database
        self._object_db = object_db
        self._ddl = ddl
        super().__init__()

    def create_database(self) -> None:
        with self._database as db:
            db.create_database(self._ddl.create.sql, self._ddl.create.args)
            msg = f"Database {self._ddl.create.name} is created."
            self._logger.info(msg)

    def drop_database(self) -> None:
        with self._database as db:
            db.drop_database(self._ddl.drop.sql, self._ddl.drop.args)
            msg = f"Database {self._ddl.create.name} is dropped."
            self._logger.info(msg)

    def create_table(self) -> None:
        with self._database as db:
            db.create_table(self._ddl.create.sql, self._ddl.create.args)
            msg = f"Table {self._ddl.create.name} is created."
            self._logger.info(msg)

    def drop_table(self) -> None:
        with self._database as db:
            db.drop_table(self._ddl.drop.sql, self._ddl.drop.args)
            msg = f"Table {self._ddl.drop.name} is dropped."
            self._logger.info(msg)

    def exists(self) -> bool:
        with self._database as db:
            exists = db.exists(self._ddl.exists.sql, self._ddl.exists.args)
            does = " exists" if exists else " does not exist."
            msg = f"Table {self._ddl.exists.name}{does}"
            self._logger.info(msg)
            return exists

    def save(self) -> None:
        with self._database as db:
            db.save()

    def reset(self, object_db: bool = True) -> None:
        self.drop_table()
        self.save()
        self.create_table()
        self.save()
        if object_db:
            self._object_db.reset()
