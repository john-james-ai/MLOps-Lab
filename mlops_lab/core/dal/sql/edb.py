#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Enter Project Name in Workspace Settings                                            #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /mlops_lab/core/dal/sql/edb.py                                                      #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : Enter URL in Workspace Settings                                                     #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday January 2nd 2023 06:32:13 am                                                 #
# Modified   : Tuesday January 24th 2023 08:13:43 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2023 John James                                                                 #
# ================================================================================================ #
import os
import dotenv
from dataclasses import dataclass

from mlops_lab.core.dal.sql.base import SQL, DDL


# ================================================================================================ #
@dataclass
class CreateDatabase(SQL):
    name: str = "mlops_lab"
    sql: str = None
    args: tuple = ()
    description: str = None

    def __post_init__(self) -> None:
        dotenv.load_dotenv()
        mode = os.getenv("MODE")
        self.sql = f"""CREATE DATABASE IF NOT EXISTS {self.name}_{mode}_events;"""
        self.description = f"Created the mlops_lab_{mode}_events database."


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DropDatabase(SQL):
    name: str = "mlops_lab"
    sql: str = None
    args: tuple = ()
    description: str = None

    def __post_init__(self) -> None:
        dotenv.load_dotenv()
        mode = os.getenv("MODE")
        self.sql = f"""DROP DATABASE IF EXISTS {self.name}_{mode}_events;"""
        self.description = f"Dropped the mlops_lab_{mode}_events database."


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DatabaseExists(SQL):
    name: str = "mlops_lab"
    sql: str = None
    args: tuple = ()
    description: str = None

    def __post_init__(self) -> None:
        dotenv.load_dotenv()
        mode = os.getenv("MODE")
        self.sql = f"""SELECT COUNT(*) FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '{self.name}_{mode}_events';"""
        self.description = f"Checked existence of the mlops_lab_{mode}_events database."


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DatabaseDDL(DDL):
    create: SQL = CreateDatabase()
    drop: SQL = DropDatabase()
    exists: SQL = DatabaseExists()
