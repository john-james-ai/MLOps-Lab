#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/dal/sql/file.py                                                        #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday December 4th 2022 06:37:18 am                                                #
# Modified   : Thursday December 29th 2022 08:19:42 pm                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
from dataclasses import dataclass
from recsys.core.dal.sql.base import SQL, DDL, DML
from recsys.core.dal.dto import DTO
# ================================================================================================ #
#                                        DATASET                                                   #
# ================================================================================================ #


# ------------------------------------------------------------------------------------------------ #
#                                          DDL                                                     #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class CreateFileTable(SQL):
    name: str = "file"
    sql: str = """CREATE TABLE IF NOT EXISTS file (id INTEGER PRIMARY KEY, oid TEXT GENERATED ALWAYS AS ('file_' || id), name TEXT NOT NULL, description TEXT, datasource TEXT NOT NULL, mode TEXT NOT NULL, stage TEXT NOT NULL, uri TEXT NOT NULL, size INTEGER DEFAULT 0, task_id INTEGER DEFAULT 0, created timestamp, modified timestamp);CREATE UNIQUE INDEX name_mode ON file(name, mode);"""
    args: tuple = ()


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DropFileTable(SQL):
    name: str = "file"
    sql: str = """DROP TABLE IF EXISTS file;"""
    args: tuple = ()


# ------------------------------------------------------------------------------------------------ #


@dataclass
class FileTableExists(SQL):
    name: str = "file"
    sql: str = """SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.name,)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class FileDDL(DDL):
    create: SQL = CreateFileTable()
    drop: SQL = DropFileTable()
    exists: SQL = FileTableExists()


# ------------------------------------------------------------------------------------------------ #
#                                          DML                                                     #
# ------------------------------------------------------------------------------------------------ #


@dataclass
class InsertFile(SQL):
    dto: DTO
    sql: str = """REPLACE INTO file (name, description, datasource, mode, stage, uri, size, task_id, created, modified) VALUES (?,?,?,?,?,?,?,?,?,?);"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (
            self.dto.name,
            self.dto.description,
            self.dto.datasource,
            self.dto.mode,
            self.dto.stage,
            self.dto.uri,
            self.dto.size,
            self.dto.task_id,
            self.dto.created,
            self.dto.modified,
        )


# ------------------------------------------------------------------------------------------------ #


@dataclass
class UpdateFile(SQL):
    dto: DTO
    sql: str = """UPDATE file SET name = ?, description = ?, datasource = ?, mode = ?, stage = ?, uri = ?, size = ?, task_id = ?, created = ?, modified = ? WHERE id = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (
            self.dto.name,
            self.dto.description,
            self.dto.datasource,
            self.dto.mode,
            self.dto.stage,
            self.dto.uri,
            self.dto.size,
            self.dto.task_id,
            self.dto.created,
            self.dto.modified,
            self.dto.id,
        )


# ------------------------------------------------------------------------------------------------ #


@dataclass
class SelectFile(SQL):
    id: int
    sql: str = """SELECT * FROM file WHERE id = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.id,)

# ------------------------------------------------------------------------------------------------ #


@dataclass
class SelectFileByNameMode(SQL):
    name: str
    mode: str
    sql: str = """SELECT * FROM file WHERE name = ? AND mode = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.name, self.mode,)


# ------------------------------------------------------------------------------------------------ #


@dataclass
class SelectAllFile(SQL):
    sql: str = """SELECT * FROM file;"""
    args: tuple = ()


# ------------------------------------------------------------------------------------------------ #


@dataclass
class FileExists(SQL):
    id: int
    sql: str = """SELECT COUNT(*) FROM file WHERE id = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.id,)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DeleteFile(SQL):
    id: int
    sql: str = """DELETE FROM file WHERE id = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.id,)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class FileDML(DML):
    insert: type(SQL) = InsertFile
    update: type(SQL) = UpdateFile
    select: type(SQL) = SelectFile
    select_by_name_mode: type(SQL) = SelectFileByNameMode
    select_all: type(SQL) = SelectAllFile
    exists: type(SQL) = FileExists
    delete: type(SQL) = DeleteFile
