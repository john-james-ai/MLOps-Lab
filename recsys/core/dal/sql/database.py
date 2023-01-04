#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/dal/sql/database.py                                                    #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday January 2nd 2023 06:32:13 am                                                 #
# Modified   : Wednesday January 4th 2023 04:43:58 am                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2023 John James                                                                 #
# ================================================================================================ #
from dataclasses import dataclass

from recsys.core.dal.sql.base import SQL, DDL


# ================================================================================================ #
@dataclass
class CreateDatabase(SQL):
    name: str = 'recsys'
    sql: str = """CREATE DATABASE recsys;"""
    args: tuple = ()
    description: str = "Created the recsys database"


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DropDatabase(SQL):
    name: str = "recsys"
    sql: str = """DROP DATABASE recsys;"""
    args: tuple = ()
    description: str = "Dropped the recsys database."


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DatabaseExists(SQL):
    name: str = "recsys"
    sql: str = """SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = 'recsys';"""
    args: tuple = ()
    description: str = "Checked existence of recsys database."


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DatabaseDDL(DDL):
    create: SQL = CreateDatabase()
    drop: SQL = DropDatabase()
    exists: SQL = DatabaseExists()

