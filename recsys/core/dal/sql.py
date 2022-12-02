#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /sql.py                                                                             #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday December 1st 2022 09:06:31 am                                              #
# Modified   : Friday December 2nd 2022 02:58:31 am                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
from datetime import datetime
import pandas as pd
from dataclasses import dataclass
from abc import ABC

# ------------------------------------------------------------------------------------------------ #
#                              DATABASE SEQUEL BASE CLASS                                          #
# ------------------------------------------------------------------------------------------------ #


@dataclass
class Sequel(ABC):
    sql: str
    args: tuple


# ================================================================================================ #
#                                        DATASET                                                   #
# ================================================================================================ #
# ------------------------------------------------------------------------------------------------ #
#                                    DATASET TABLE DDL                                             #
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
    table: str = None
    sql: str = """SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.table,)


# ------------------------------------------------------------------------------------------------ #
#                                   DATASET DML                                                    #
# ------------------------------------------------------------------------------------------------ #


@dataclass
class DatasetIdExists(Sequel):
    id: int = None
    sql: str = """SELECT COUNT(*) FROM dataset WHERE id = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.id,)


# ------------------------------------------------------------------------------------------------ #


@dataclass
class DatasetExists(Sequel):
    name: int = None
    source: str = None
    env: str = None
    stage: str = None
    version: int = None
    sql: str = """SELECT COUNT(*) FROM dataset WHERE source = ? AND env = ? AND name = ? AND stage = ? AND version = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (
            self.source,
            self.env,
            self.name,
            self.stage,
            self.version,
        )


# ------------------------------------------------------------------------------------------------ #
@dataclass
class FindDataset(Sequel):
    source: str = None
    env: str = None
    name: str = None
    stage: str = None
    args: tuple = ()
    sql: str = """SELECT * FROM dataset WHERE source = ? AND env = ? AND name = ? AND stage = ?;"""

    def __post_init__(self) -> None:
        self.args = (
            self.source,
            self.env,
            self.name,
            self.stage,
        )


# ------------------------------------------------------------------------------------------------ #


@dataclass
class SelectDataset(Sequel):
    id: int = None
    sql: str = """SELECT * FROM dataset WHERE id = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.id,)


# ------------------------------------------------------------------------------------------------ #


@dataclass
class ListDatasets(Sequel):
    sql: str = """SELECT * FROM dataset WHERE archived = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (0,)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class ListSourceDatasets(Sequel):
    source: str = None
    args: tuple = ()
    sql: str = """SELECT * FROM dataset WHERE source = ?;"""

    def __post_init__(self) -> None:
        self.args = (self.source,)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class ListEnvDatasets(Sequel):
    env: str = None
    args: tuple = ()
    sql: str = """SELECT * FROM dataset WHERE env = ?;"""

    def __post_init__(self) -> None:
        self.args = (self.env,)


# ------------------------------------------------------------------------------------------------ #


@dataclass
class ArchiveDataset(Sequel):
    id: int = None
    filepath: str = None
    sql: str = """UPDATE dataset SET archived = ?, filepath = ? WHERE id = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (
            True,
            self.filepath,
            self.id,
        )


# ------------------------------------------------------------------------------------------------ #


@dataclass
class RestoreDataset(Sequel):
    id: int = None
    filepath: str = None
    sql: str = """UPDATE dataset SET archived = ?, filepath = ? WHERE id = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (
            False,
            self.filepath,
            self.id,
        )


# ------------------------------------------------------------------------------------------------ #


@dataclass
class InsertDataset(Sequel):
    """All attributes of a Dataset are included; however, two are not used - namely id, and data."""

    id: int = None
    source: str = None
    env: str = None
    name: str = None
    description: str = None
    data: pd.DataFrame = None
    stage: str = None
    version: int = None
    cost: int = None
    nrows: int = None
    ncols: int = None
    null_counts: int = None
    memory_size_mb: int = None
    filepath: str = None
    task_id: int = None
    step_id: int = None
    created: datetime = None
    is_archived: bool = None
    archived: datetime = None

    sql: str = """INSERT INTO dataset (source,env,name,description,data,stage,version,cost,nrows,ncols,null_counts,memory_size_mb,filepath,task_id,step_id,created,is_archived,archived) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (
            self.source,
            self.env,
            self.name,
            self.description,
            self.data,
            self.stage,
            self.version,
            self.cost,
            self.nrows,
            self.ncols,
            self.null_counts,
            self.memory_size_mb,
            self.filepath,
            self.task_id,
            self.step_id,
            self.created,
            self.is_archived,
            self.archived,
        )


# ------------------------------------------------------------------------------------------------ #
@dataclass
class CountDatasets(Sequel):
    sql: str = """SELECT COUNT(*) FROM dataset;"""
    args: tuple = ()


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DeleteDataset(Sequel):
    id: int = None
    sql: str = """DELETE FROM dataset WHERE id = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.id,)


# ------------------------------------------------------------------------------------------------ #
