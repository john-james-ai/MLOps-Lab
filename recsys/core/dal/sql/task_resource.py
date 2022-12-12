#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/dal/sql/task_resource.py                                               #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday December 4th 2022 06:37:18 am                                                #
# Modified   : Sunday December 11th 2022 02:55:01 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
from dataclasses import dataclass
from recsys.core.dal.base import SQL, DDL, DML
from recsys.core.dal.dto import DTO

# ================================================================================================ #
#                                       TASK RESOURCE                                              #
# ================================================================================================ #


# ------------------------------------------------------------------------------------------------ #
#                                            DDL                                                   #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class CreateTaskResourceTable(SQL):
    name: str = "task_resource"
    sql: str = """CREATE TABLE IF NOT EXISTS task_resource (id INTEGER PRIMARY KEY, name TEXT NOT NULL, description TEXT, task_id INTEGER NOT NULL, resource_kind TEXT NOT NULL, resource_id INTEGER NOT NULL, resource_context TEXT NOT NULL, created timestamp, modified timestamp);"""
    args: tuple = ()


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DropTaskResourceTable(SQL):
    name: str = "task_resource"
    sql: str = """DROP TABLE IF EXISTS task_resource;"""
    args: tuple = ()


# ------------------------------------------------------------------------------------------------ #


@dataclass
class TaskResourceTableExists(SQL):
    name: str = "task_resource"
    sql: str = """SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.name,)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class TaskResourceDDL(DDL):
    create: SQL = CreateTaskResourceTable()
    drop: SQL = DropTaskResourceTable()
    exists: SQL = TaskResourceTableExists()


# ------------------------------------------------------------------------------------------------ #
#                                          DML                                                     #
# ------------------------------------------------------------------------------------------------ #


@dataclass
class InsertTaskResource(SQL):
    dto: DTO
    sql: str = """INSERT INTO task_resource (name, description, task_id, resource_kind, resource_id, resource_context, created, modified) VALUES (?,?,?,?,?,?,?,?);"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (
            self.dto.name,
            self.dto.description,
            self.dto.task_id,
            self.dto.resource_kind,
            self.dto.resource_id,
            self.dto.resource_context,
            self.dto.created,
            self.dto.modified,
        )


# ------------------------------------------------------------------------------------------------ #


@dataclass
class UpdateTaskResource(SQL):
    dto: DTO
    sql: str = """UPDATE task_resource SET name = ?, description = ?, task_id = ?,  resource_kind = ?, resource_id = ?, resource_context = ?, created = ?, modified = ? WHERE id = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (
            self.dto.name,
            self.dto.description,
            self.dto.task_id,
            self.dto.resource_kind,
            self.dto.resource_id,
            self.dto.resource_context,
            self.dto.created,
            self.dto.modified,
            self.dto.id,
        )


# ------------------------------------------------------------------------------------------------ #


@dataclass
class SelectTaskResource(SQL):
    id: int
    sql: str = """SELECT * FROM task_resource WHERE id = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.id,)


# ------------------------------------------------------------------------------------------------ #


@dataclass
class SelectAllTaskResources(SQL):
    sql: str = """SELECT * FROM task_resource;"""
    args: tuple = ()


# ------------------------------------------------------------------------------------------------ #

@dataclass
class TaskResourceExists(SQL):
    id: int
    sql: str = """SELECT COUNT(*) FROM task_resource WHERE id = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.id,)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DeleteTaskResource(SQL):
    id: int
    sql: str = """DELETE FROM task_resource WHERE id = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.id,)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class TaskResourceDML(DML):
    insert: type(SQL) = InsertTaskResource
    update: type(SQL) = UpdateTaskResource
    select: type(SQL) = SelectTaskResource
    select_all: type(SQL) = SelectAllTaskResources
    exists: type(SQL) = TaskResourceExists
    delete: type(SQL) = DeleteTaskResource
