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
# Modified   : Tuesday December 6th 2022 06:06:40 am                                               #
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
    sql: str = """CREATE TABLE IF NOT EXISTS fileset (id INTEGER PRIMARY KEY, name TEXT NOT NULL, description TEXT, source TEXT NOT NULL, filesize REAL NOT NULL, filepath TEXT NOT NULL, task_id INTEGER DEFAULT (0), creator TEXT NOT NULL, created TEXT DEFAULT (datetime('now')), modified TEXT DEFAULT (datetime('now')));"""
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

    sql: str = """INSERT INTO fileset (name, description, source, filesize, filepath, task_id, creator, created, modified) VALUES (?,?,?,?,?,?,?,?,?);"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (
            self.dto.name,
            self.dto.description,
            self.dto.source,
            self.dto.filesize,
            self.dto.filepath,
            self.dto.task_id,
            self.dto.creator,
            self.dto.created,
            self.dto.modified,
        )


# ------------------------------------------------------------------------------------------------ #


@dataclass
class UpdateFileset(SQL):
    dto: DTO
    sql: str = """UPDATE fileset SET name = ?, description = ?, source = ?, filesize = ?, filepath = ?, task_id = ?, creator = ?, created = ?, modified = ? WHERE id = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (
            self.dto.name,
            self.dto.description,
            self.dto.source,
            self.dto.filesize,
            self.dto.filepath,
            self.dto.task_id,
            self.dto.creator,
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
    select_all: type(SQL) = SelectAllFilesets
    exists: type(SQL) = FilesetExists
    delete: type(SQL) = DeleteFileset
