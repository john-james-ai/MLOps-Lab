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
# Modified   : Sunday November 27th 2022 04:31:21 am                                               #
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
    sql: str = """CREATE TABLE IF NOT EXISTS dataset_registry ( id INTEGER PRIMARY KEY, name TEXT NOT NULL, description TEXT NOT NULL, stage TEXT NOT NULL, version INTEGER NOT NULL, cost INTEGER NOT NULL, nrows INTEGER NOT NULL, ncols INTEGER NOT NULL, null_counts INTEGER, memory_size_mb TEXT NOT NULL, filepath TEXT, archived INTEGER DEFAULT 0,  creator TEXT NOT NULL, created TEXT DEFAULT (datetime('now')));"""
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
    args: tuple = ()

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
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (
            self.name,
            self.stage,
            self.version,
        )


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
class SelectCurrentDatasets:
    sql: str = """SELECT * FROM dataset_registry WHERE archived = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (0,)


# ------------------------------------------------------------------------------------------------ #


@dataclass
class SelectArchivedDatasets:
    sql: str = """SELECT * FROM dataset_registry WHERE archived = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (1,)


# ------------------------------------------------------------------------------------------------ #


@dataclass
class ArchiveDataset:
    id: int
    sql: str = """UPDATE dataset_registry SET archived = ? WHERE id = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (
            True,
            self.id,
        )


# ------------------------------------------------------------------------------------------------ #


@dataclass
class RestoreDataset:
    id: int
    sql: str = """UPDATE dataset_registry SET archived = ? WHERE id = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (
            False,
            self.id,
        )


# ------------------------------------------------------------------------------------------------ #


@dataclass
class InsertDataset:
    """All attributes of a Dataset are included; however, two are not used - namely id, and data."""

    id: int
    name: str
    description: str
    data: bool
    stage: str
    version: int
    cost: int
    nrows: int
    ncols: int
    null_counts: int
    memory_size_mb: int
    filepath: str
    archived: bool
    creator: str
    created: str

    sql: str = """INSERT INTO dataset_registry (name, description, stage, version, cost, nrows, ncols, null_counts, memory_size_mb, filepath, archived, creator, created) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"""
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
            self.memory_size_mb,
            self.filepath,
            self.archived,
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
