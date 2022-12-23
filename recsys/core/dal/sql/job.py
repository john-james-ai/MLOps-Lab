#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/dal/sql/job.py                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday December 4th 2022 06:37:18 am                                                #
# Modified   : Monday December 19th 2022 09:12:38 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
from dataclasses import dataclass
from recsys.core.dal.sql.base import SQL, DDL, DML
from recsys.core.dal.dto import DTO

# ================================================================================================ #
#                                         JOB                                                      #
# ================================================================================================ #


# ------------------------------------------------------------------------------------------------ #
#                                          DDL                                                     #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class CreateJobTable(SQL):
    name: str = "job"
    sql: str = """CREATE TABLE IF NOT EXISTS job (id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE, description TEXT, mode TEXT NOT NULL, started timestamp, ended timestamp, duration REAL, state TEXT DEFAULT "CREATED", created timestamp, modified timestamp);"""
    args: tuple = ()


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DropJobTable(SQL):
    name: str = "job"
    sql: str = """DROP TABLE IF EXISTS job;"""
    args: tuple = ()


# ------------------------------------------------------------------------------------------------ #


@dataclass
class JobTableExists(SQL):
    name: str = "job"
    sql: str = """SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.name,)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class JobDDL(DDL):
    create: SQL = CreateJobTable()
    drop: SQL = DropJobTable()
    exists: SQL = JobTableExists()


# ------------------------------------------------------------------------------------------------ #
#                                          DML                                                     #
# ------------------------------------------------------------------------------------------------ #


@dataclass
class InsertJob(SQL):
    dto: DTO

    sql: str = """INSERT INTO job (name, description, mode, started, ended, duration, state, created, modified) VALUES (?,?,?,?,?,?,?,?,?);"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (
            self.dto.name,
            self.dto.description,
            self.dto.mode,
            self.dto.started,
            self.dto.ended,
            self.dto.duration,
            self.dto.state,
            self.dto.created,
            self.dto.modified,
        )


# ------------------------------------------------------------------------------------------------ #


@dataclass
class UpdateJob(SQL):
    dto: DTO
    sql: str = """UPDATE job SET name = ?, description = ?, mode = ?, started = ?, ended = ?, duration = ?, state = ?, created = ?, modified = ? WHERE id = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (
            self.dto.name,
            self.dto.description,
            self.dto.mode,
            self.dto.started,
            self.dto.ended,
            self.dto.duration,
            self.dto.state,
            self.dto.created,
            self.dto.modified,
            self.dto.id,
        )


# ------------------------------------------------------------------------------------------------ #


@dataclass
class SelectJob(SQL):
    id: int
    sql: str = """SELECT * FROM job WHERE id = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.id,)

# ------------------------------------------------------------------------------------------------ #


@dataclass
class SelectJobByName(SQL):
    name: str
    sql: str = """SELECT * FROM job WHERE name = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.name,)


# ------------------------------------------------------------------------------------------------ #


@dataclass
class SelectAllJob(SQL):
    sql: str = """SELECT * FROM job;"""
    args: tuple = ()

# ------------------------------------------------------------------------------------------------ #


@dataclass
class JobExists(SQL):
    id: int
    sql: str = """SELECT COUNT(*) FROM job WHERE id = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.id,)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DeleteJob(SQL):
    id: int
    sql: str = """DELETE FROM job WHERE id = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.id,)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class JobDML(DML):
    insert: type(SQL) = InsertJob
    update: type(SQL) = UpdateJob
    select: type(SQL) = SelectJob
    select_by_name: type(SQL) = SelectJobByName
    select_all: type(SQL) = SelectAllJob
    exists: type(SQL) = JobExists
    delete: type(SQL) = DeleteJob
