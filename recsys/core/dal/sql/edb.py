#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/dal/sql/edb.py                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday January 2nd 2023 06:32:13 am                                                 #
# Modified   : Friday January 13th 2023 05:54:32 pm                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2023 John James                                                                 #
# ================================================================================================ #
import os
import dotenv
from dataclasses import dataclass

from recsys.core.dal.sql.base import SQL, DDL


# ================================================================================================ #
@dataclass
class CreateDatabase(SQL):
    name: str = "recsys"
    sql: str = None
    args: tuple = ()
    description: str = None

    def __post_init__(self) -> None:
        dotenv.load_dotenv()
        mode = os.getenv("MODE")
        self.sql = f"""CREATE DATABASE IF NOT EXISTS {self.name}_{mode}_events;"""
        self.description = f"Created the recsys_{mode}_events database."


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DropDatabase(SQL):
    name: str = "recsys"
    sql: str = None
    args: tuple = ()
    description: str = None

    def __post_init__(self) -> None:
        dotenv.load_dotenv()
        mode = os.getenv("MODE")
        self.sql = f"""DROP DATABASE IF EXISTS {self.name}_{mode}_events;"""
        self.description = f"Dropped the recsys_{mode}_events database."


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DatabaseExists(SQL):
    name: str = "recsys"
    sql: str = None
    args: tuple = ()
    description: str = None

    def __post_init__(self) -> None:
        dotenv.load_dotenv()
        mode = os.getenv("MODE")
        self.sql = f"""SELECT COUNT(*) FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '{self.name}_{mode}_events';"""
        self.description = f"Checked existence of the recsys_{mode}_events database."


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DatabaseDDL(DDL):
    create: SQL = CreateDatabase()
    drop: SQL = DropDatabase()
    exists: SQL = DatabaseExists()
