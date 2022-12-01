#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /admin_ddl.py                                                                       #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday December 1st 2022 09:06:31 am                                              #
# Modified   : Thursday December 1st 2022 10:41:51 am                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
from dataclasses import dataclass

from .base import Sequel

# ------------------------------------------------------------------------------------------------ #
#                                 DATASET TABLE DDL                                                #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class CreateDatasetTable(Sequel):
    sql: str = """CREATE TABLE IF NOT EXISTS dataset ( id INTEGER PRIMARY KEY, source TEXT NOT NULL, env TEXT NOT NULL, stage TEXT NOT NULL, name TEXT NOT NULL, description TEXT NOT NULL,  version INTEGER NOT NULL, cost INTEGER NOT NULL, nrows INTEGER NOT NULL, ncols INTEGER NOT NULL, null_counts INTEGER, memory_size_mb TEXT NOT NULL, filepath TEXT, archived INTEGER DEFAULT 0,  creator TEXT NOT NULL, created TEXT DEFAULT (datetime('now')));"""
    args: tuple = ()


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DropDatasetTable(Sequel):
    sql: str = """DROP TABLE IF EXISTS dataset;"""
    args: tuple = ()


# ------------------------------------------------------------------------------------------------ #


@dataclass
class TableExists(Sequel):
    table: str
    sql: str = """SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.table,)
