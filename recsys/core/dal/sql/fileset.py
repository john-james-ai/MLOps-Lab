#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/dal/sql/fileset.py                                                     #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday December 4th 2022 06:37:18 am                                                #
# Modified   : Tuesday December 13th 2022 07:04:42 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
from dataclasses import dataclass
from recsys.core.dal.base import SQL, DDL, DML
from recsys.core.dal.dto import DTO


# ================================================================================================ #
#                                        FILESET                                                   #
# ================================================================================================ #


# ------------------------------------------------------------------------------------------------ #
#                                          DDL                                                     #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class CreateFilesetTable(SQL):
    name: str = "fileset"
    sql: str = """CREATE TABLE IF NOT EXISTS fileset (id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE, description TEXT, datasource TEXT NOT NULL, workspace TEXT NOT NULL, stage TEXT NOT NULL, uri TEXT NOT NULL, filesize INTEGER, dataset_id INTEGER, task_id INTEGER DEFAULT 0, created timestamp, modified timestamp);"""
    args: tuple = ()


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DropFilesetTable(SQL):
    name: str = "fileset"
    sql: str = """DROP TABLE IF EXISTS fileset;"""
    args: tuple = ()


# ------------------------------------------------------------------------------------------------ #


@dataclass
class FilesetTableExists(SQL):
    name: str = "fileset"
    sql: str = """SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.name,)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class FilesetDDL(DDL):
    create: SQL = CreateFilesetTable()
    drop: SQL = DropFilesetTable()
    exists: SQL = FilesetTableExists()


# ------------------------------------------------------------------------------------------------ #
#                                          DML                                                     #
# ------------------------------------------------------------------------------------------------ #


@dataclass
class InsertFileset(SQL):
    dto: DTO

    sql: str = """INSERT INTO fileset (name, description, datasource, workspace, stage, uri, filesize, dataset_id, task_id, created, modified) VALUES (?,?,?,?,?,?,?,?,?,?,?);"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (
            self.dto.name,
            self.dto.description,
            self.dto.datasource,
            self.dto.workspace,
            self.dto.stage,
            self.dto.uri,
            self.dto.filesize,
            self.dto.dataset_id,
            self.dto.task_id,
            self.dto.created,
            self.dto.modified,
        )


# ------------------------------------------------------------------------------------------------ #


@dataclass
class UpdateFileset(SQL):
    dto: DTO
    sql: str = """UPDATE fileset SET name = ?, description = ?, datasource = ?, workspace = ?, stage = ?, uri = ?, filesize = ?, dataset_id = ?, task_id = ?, created = ?, modified = ? WHERE id = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (
            self.dto.name,
            self.dto.description,
            self.dto.datasource,
            self.dto.workspace,
            self.dto.stage,
            self.dto.uri,
            self.dto.filesize,
            self.dto.dataset_id,
            self.dto.task_id,
            self.dto.created,
            self.dto.modified,
            self.dto.id,
        )


# ------------------------------------------------------------------------------------------------ #


@dataclass
class SelectFileset(SQL):
    id: int
    sql: str = """SELECT * FROM fileset WHERE id = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.id,)

# ------------------------------------------------------------------------------------------------ #


@dataclass
class SelectFilesetByName(SQL):
    name: str
    sql: str = """SELECT * FROM fileset WHERE name = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.name,)


# ------------------------------------------------------------------------------------------------ #


@dataclass
class SelectAllFilesets(SQL):
    sql: str = """SELECT * FROM fileset;"""
    args: tuple = ()

# ------------------------------------------------------------------------------------------------ #


@dataclass
class FilesetExists(SQL):
    id: int
    sql: str = """SELECT COUNT(*) FROM fileset WHERE id = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.id,)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DeleteFileset(SQL):
    id: int
    sql: str = """DELETE FROM fileset WHERE id = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.id,)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class FilesetDML(DML):
    insert: type(SQL) = InsertFileset
    update: type(SQL) = UpdateFileset
    select: type(SQL) = SelectFileset
    select_by_name: type(SQL) = SelectFilesetByName
    select_all: type(SQL) = SelectAllFilesets
    exists: type(SQL) = FilesetExists
    delete: type(SQL) = DeleteFileset
