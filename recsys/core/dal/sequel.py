#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /sequel.py                                                                          #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday November 22nd 2022 02:47:16 am                                              #
# Modified   : Friday November 25th 2022 01:06:44 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""SQL Module"""
from dataclasses import dataclass


# ------------------------------------------------------------------------------------------------ #
#                                     DATA DEFINITION                                              #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class CreateDatasetRegistryTable:
    sql: str = """CREATE TABLE IF NOT EXISTS dataset_registry ( id INTEGER PRIMARY KEY, name TEXT NOT NULL, description TEXT NOT NULL, stage TEXT NOT NULL, version INTEGER NOT NULL, cost INTEGER NOT NULL, nrows TEXT NOT NULL, ncols INTEGER NOT NULL, null_counts INTEGER, memory_size TEXT NOT NULL, filepath TEXT, creator TEXT NOT NULL, created DATETIME NOT NULL );"""
    args: tuple = ()


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DropDatasetRegistryTable:
    sql: str = """DROP TABLE IF EXISTS dataset_registry;"""
    args: tuple = ()


# ------------------------------------------------------------------------------------------------ #


@dataclass
class TableExists:
    table: str
    sql: str = """SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.table,)


# ------------------------------------------------------------------------------------------------ #
#                                    REGISTRY TABLE                                                #
# ------------------------------------------------------------------------------------------------ #


@dataclass
class DatasetExists:
    id: int
    sql: str = """SELECT COUNT(*) FROM dataset_registry WHERE id = ?;"""

    def __post_init__(self) -> None:
        self.args = (self.id,)


# ------------------------------------------------------------------------------------------------ #


@dataclass
class VersionExists:
    name: int
    stage: str
    version: int
    sql: str = (
        """SELECT COUNT(*) FROM dataset_registry WHERE name = ? AND stage = ? AND version = ?;"""
    )

    def __post_init__(self) -> None:
        self.args = (self.name, self.stage, self.version)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class FindDatasetByNameStage:
    name: str
    stage: str
    args: tuple = ()
    sql: str = """SELECT * FROM dataset_registry WHERE name = ? AND stage = ?;"""

    def __post_init__(self) -> None:
        self.args = (
            self.name,
            self.stage,
        )


# ------------------------------------------------------------------------------------------------ #
@dataclass
class FindDatasetByName:
    name: str
    args: tuple = ()
    sql: str = """SELECT * FROM dataset_registry WHERE name = ?;"""

    def __post_init__(self) -> None:
        self.args = (self.name,)


# ------------------------------------------------------------------------------------------------ #


@dataclass
class SelectDataset:
    id: int
    sql: str = """SELECT * FROM dataset_registry WHERE id = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.id,)


# ------------------------------------------------------------------------------------------------ #


@dataclass
class SelectAllDatasets:
    sql: str = """SELECT * FROM dataset_registry;"""
    args: tuple = ()


# ------------------------------------------------------------------------------------------------ #


@dataclass
class InsertDataset:
    name: str
    description: str
    stage: str
    version: str
    cost: str
    nrows: str
    ncols: str
    null_counts: str
    memory_size: str
    filepath: str
    creator: str
    created: str

    sql: str = """INSERT INTO dataset_registry (name, description, stage, version, cost, nrows, ncols, null_counts, memory_size, filepath, creator, created) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (
            self.name,
            self.description,
            self.stage,
            self.version,
            self.cost,
            self.nrows,
            self.ncols,
            self.null_counts,
            self.memory_size,
            self.filepath,
            self.creator,
            self.created,
        )


# ------------------------------------------------------------------------------------------------ #
@dataclass
class CountDatasets:
    sql: str = """SELECT COUNT(*) FROM dataset_registry;"""
    args: tuple = ()


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DeleteDataset:
    id: int
    sql: str = """DELETE FROM dataset_registry WHERE id = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.id,)


# ------------------------------------------------------------------------------------------------ #
