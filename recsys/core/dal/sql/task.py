#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/dal/sql/task.py                                                        #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday December 4th 2022 06:37:18 am                                                #
# Modified   : Friday December 16th 2022 11:29:55 am                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
from dataclasses import dataclass
from recsys.core.dal.sql.base import SQL, DDL, DML
from recsys.core.dal.dto import DTO

# ================================================================================================ #
#                                           TASK                                                   #
# ================================================================================================ #


# ------------------------------------------------------------------------------------------------ #
#                                            DDL                                                   #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class CreateTaskTable(SQL):
    name: str = "task"
    sql: str = """CREATE TABLE IF NOT EXISTS task (id INTEGER PRIMARY KEY, name TEXT NOT NULL, description TEXT, workspace TEXT NOT NULL, operator TEXT NOT NULL, started timestamp, ended timestamp, duration REAL, job_id INTEGER DEFAULT 0, created timestamp, modified timestamp);"""
    args: tuple = ()


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DropTaskTable(SQL):
    name: str = "task"
    sql: str = """DROP TABLE IF EXISTS task;"""
    args: tuple = ()


# ------------------------------------------------------------------------------------------------ #


@dataclass
class TaskTableExists(SQL):
    name: str = "task"
    sql: str = """SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.name,)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class TaskDDL(DDL):
    create: SQL = CreateTaskTable()
    drop: SQL = DropTaskTable()
    exists: SQL = TaskTableExists()


# ------------------------------------------------------------------------------------------------ #
#                                          DML                                                     #
# ------------------------------------------------------------------------------------------------ #


@dataclass
class InsertTask(SQL):
    """All attributes of a Task are included; however, two are not used - namely id, and data."""
    dto: DTO
    sql: str = """INSERT INTO task (name, description, workspace, operator, started, ended, duration, job_id, created, modified) VALUES (?,?,?,?,?,?,?,?,?,?);"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (
            self.dto.name,
            self.dto.description,
            self.dto.workspace,
            self.dto.operator,
            self.dto.started,
            self.dto.ended,
            self.dto.duration,
            self.dto.job_id,
            self.dto.created,
            self.dto.modified,
        )


# ------------------------------------------------------------------------------------------------ #


@dataclass
class UpdateTask(SQL):
    dto: DTO
    sql: str = """UPDATE task SET name = ?, description = ?, workspace = ?, operator = ?, started = ?, ended = ?, duration = ?, job_id = ?, created = ?, modified = ? WHERE id = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (
            self.dto.name,
            self.dto.description,
            self.dto.workspace,
            self.dto.operator,
            self.dto.started,
            self.dto.ended,
            self.dto.duration,
            self.dto.job_id,
            self.dto.created,
            self.dto.modified,
            self.dto.id,
        )


# ------------------------------------------------------------------------------------------------ #


@dataclass
class SelectTask(SQL):
    id: int
    sql: str = """SELECT * FROM task WHERE id = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.id,)

# ------------------------------------------------------------------------------------------------ #


@dataclass
class SelectTaskByName(SQL):
    name: str
    sql: str = """SELECT * FROM task WHERE name = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.name,)


# ------------------------------------------------------------------------------------------------ #


@dataclass
class SelectAllTasks(SQL):
    sql: str = """SELECT * FROM task;"""
    args: tuple = ()


# ------------------------------------------------------------------------------------------------ #

@dataclass
class TaskExists(SQL):
    id: int
    sql: str = """SELECT COUNT(*) FROM task WHERE id = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.id,)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DeleteTask(SQL):
    id: int
    sql: str = """DELETE FROM task WHERE id = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.id,)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class TaskDML(DML):
    insert: type(SQL) = InsertTask
    update: type(SQL) = UpdateTask
    select: type(SQL) = SelectTask
    select_by_name: type(SQL) = SelectTaskByName
    select_all: type(SQL) = SelectAllTasks
    exists: type(SQL) = TaskExists
    delete: type(SQL) = DeleteTask
