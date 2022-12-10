#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/dal/ddo.py                                                             #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday December 4th 2022 06:27:36 am                                                #
# Modified   : Friday December 9th 2022 07:38:04 pm                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Data Definition Object Module."""

from recsys.core import Service
from recsys.core.data.database import Database
from .base import DDL
# ------------------------------------------------------------------------------------------------ #

# ------------------------------------------------------------------------------------------------ #
#                                       TABLE SERVICE                                              #
# ------------------------------------------------------------------------------------------------ #


class TableService(Service):
    def __init__(self, database: Database, ddl: DDL) -> None:
        self._database = database
        self._ddl = ddl
        super().__init__()

    def create(self) -> None:
        self._logger.debug(self._database)
        with self._database as db:
            db.create_table(self._ddl.create.sql, self._ddl.create.args)
            self._logger.info(f"Created {self._ddl.create.name} table.")

    def drop(self) -> None:
        self._logger.debug(self._database)
        with self._database as db:
            db.drop_table(self._ddl.drop.sql, self._ddl.drop.args)
            self._logger.info(f"Dropped {self._ddl.drop.name} table.")

    def exists(self) -> bool:
        self._logger.debug(self._database)
        with self._database as db:
            exists = db.exists(self._ddl.exists.sql, self._ddl.exists.args)
            does = " does " if exists else " does not "
            msg = f"Table {self._ddl.exists.name}{does} exist."
            self._logger.info(msg)
            return exists

    def save(self) -> None:
        self._logger.debug(self._database)
        with self._database as db:
            db.save()

    def reset(self) -> None:
        self.drop()
        self.save()
        self.create()
        self.save()
