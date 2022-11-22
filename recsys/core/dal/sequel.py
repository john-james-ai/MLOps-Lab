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
# Modified   : Tuesday November 22nd 2022 04:26:51 am                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""SQL Module"""
from dataclasses import dataclass

# ------------------------------------------------------------------------------------------------ #


@dataclass
class CreateDatasetRegistryTable:
    sql: str = """CREATE TABLE IF NOT EXISTS dataset.registry (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        description TEXT NOT NULL,
        env TEXT NOT NULL,
        stage TEXT NOT NULL,
        cost INTEGER NOT NULL,
        version INTEGER NOT NULL,
        creator TEXT NOT NULL,
        created DATETIME NOT NULL,
        nrows INTEGER NOT NULL,
        ncols INTEGER NOT NULL,
        null_counts INTEGER NOT NULL,
        memory_size INTEGER NOT NULL
        );
        """
    args: tuple = None


@dataclass
class DropDatasetRegistryTable:
    sql = """DROP TABLE IF EXISTS dataset.registry;"""
    args: tuple = None


@dataclass
class DatabaseRegistryTableExists:
    sql: str = """SELECT name FROM sqlite_master WHERE type='table' AND name=?;"""
    params: tuple = "dataset.registry"


@dataclass
class DatasetExists:
    name: str
    env: str
    stage: str
    version: int
    args: tuple = None
    sql: str = """
    SELECT COUNT(*)
    FROM dataset.registry
    WHERE
        name = ? AND
        env = ? AND
        stage = ? AND
        version = ?;
    """

    def __post_init__(self) -> None:
        self.args = (self.name, self.env, self.stage, self.version)


@dataclass
class SelectDatasetRegistration:
    id: int
    sql: str = """SELECT * FROM dataset.registry WHERE id = ?;"""
    args: tuple = None

    def __post_init__(self) -> None:
        self.args = (self.id,)


@dataclass
class InsertDatasetRegistration:
    name: str
    description: str
    env: str
    stage: str
    cost: str
    version: str
    creator: str
    created: str
    nrows: str
    ncols: str
    null_counts: str
    memory_size: str

    sql: str = """INSERT INTO dataset.registry
        (name, description, env, stage, cost, version, nrows, ncols, null_counts, memory_size, creator, created)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"""
    args: tuple = None

    def __post_init__(self) -> None:
        self.args = (
            self.name,
            self.description,
            self.env,
            self.stage,
            self.cost,
            self.version,
            self.nrows,
            self.ncols,
            self.null_counts,
            self.memory_size,
            self.creator,
            self.created,
        )


@dataclass
class RemoveDatasetRegistration:
    id: int
    sql: str = """DELETE FROM dataset.registry WHERE id = ?;"""
    args: tuple = None

    def __post_init__(self) -> None:
        self.args = (self.id,)
