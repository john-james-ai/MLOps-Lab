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
# Modified   : Sunday January 22nd 2023 02:19:23 pm                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import os
import dotenv

from dataclasses import dataclass
from recsys.core.dal.sql.base import SQL, DDL, DML
from recsys.core.dal.dto import DTO
from recsys.core.entity.base import Entity
from recsys.core.workflow.dag import Task

# ================================================================================================ #
#                                           TASK                                                   #
# ================================================================================================ #


# ------------------------------------------------------------------------------------------------ #
#                                            DDL                                                   #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class CreateTaskTable(SQL):
    name: str = "task"
    sql: str = """CREATE TABLE IF NOT EXISTS task (id MEDIUMINT PRIMARY KEY AUTO_INCREMENT, oid VARCHAR(255) NOT NULL, name VARCHAR(128) NOT NULL, description VARCHAR(255), state VARCHAR(32), dag_oid VARCHAR(128) NOT NULL, created DATETIME DEFAULT CURRENT_TIMESTAMP, modified DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, UNIQUE(name));"""
    args: tuple = ()
    description: str = "Created the task table."


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DropTaskTable(SQL):
    name: str = "task"
    sql: str = """DROP TABLE IF EXISTS task;"""
    args: tuple = ()
    description: str = "Dropped the task table."


# ------------------------------------------------------------------------------------------------ #


@dataclass
class TaskTableExists(SQL):
    name: str = "task"
    sql: str = None
    args: tuple = ()
    description: str = "Checked existence of task table."

    def __post_init__(self) -> None:
        dotenv.load_dotenv()
        mode = os.getenv("MODE")
        self.sql = f"""SELECT COUNT(TABLE_NAME) FROM information_schema.TABLES WHERE TABLE_SCHEMA LIKE 'recsys_{mode}_events' AND TABLE_NAME = 'task';"""


# ------------------------------------------------------------------------------------------------ #
@dataclass
class TaskDDL(DDL):
    entity: type[Entity] = Task
    create: SQL = CreateTaskTable()
    drop: SQL = DropTaskTable()
    exists: SQL = TaskTableExists()


# ------------------------------------------------------------------------------------------------ #
#                                          DML                                                     #
# ------------------------------------------------------------------------------------------------ #


@dataclass
class InsertTask(SQL):
    dto: DTO
    sql: str = (
        """INSERT INTO task (oid, name, description, state, dag_oid) VALUES (%s, %s, %s, %s, %s);"""
    )
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (
            self.dto.oid,
            self.dto.name,
            self.dto.description,
            self.dto.state,
            self.dto.dag_oid,
        )


# ------------------------------------------------------------------------------------------------ #


@dataclass
class UpdateTask(SQL):
    dto: DTO
    sql: str = """UPDATE task SET oid = %s, name = %s, description = %s, state = %s, dag_oid = %s WHERE id = %s;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (
            self.dto.oid,
            self.dto.name,
            self.dto.description,
            self.dto.state,
            self.dto.dag_oid,
            self.dto.id,
        )


# ------------------------------------------------------------------------------------------------ #


@dataclass
class SelectTask(SQL):
    id: int
    sql: str = """SELECT * FROM task WHERE id = %s;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.id,)


# ------------------------------------------------------------------------------------------------ #


@dataclass
class SelectTaskByParentOid(SQL):
    dag_oid: str
    sql: str = """SELECT * FROM task WHERE dag_oid = %s;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.dag_oid,)


# ------------------------------------------------------------------------------------------------ #


@dataclass
class SelectTaskByName(SQL):
    name: str
    sql: str = """SELECT * FROM task WHERE name = %s;"""
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
    sql: str = """SELECT EXISTS(SELECT 1 FROM task WHERE id = %s LIMIT 1);"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.id,)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DeleteTask(SQL):
    id: int
    sql: str = """DELETE FROM task WHERE id = %s;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.id,)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class LoadTask(SQL):
    filename: str
    tablename: str = "task"
    sql: str = None
    args: tuple = ()

    def __post_init__(self) -> None:
        self.sql = f"""LOAD DATA LOCAL INFILE '{self.filename}' INTO TABLE {self.tablename} FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\r\n' IGNORE 1 ROWS;"""


# ------------------------------------------------------------------------------------------------ #
@dataclass
class TaskDML(DML):
    entity: type[Entity] = Task
    insert: type[SQL] = InsertTask
    update: type[SQL] = UpdateTask
    select: type[SQL] = SelectTask
    select_by_name: type[SQL] = SelectTaskByName
    select_by_dag_oid: type[SQL] = SelectTaskByParentOid
    select_all: type[SQL] = SelectAllTasks
    exists: type[SQL] = TaskExists
    delete: type[SQL] = DeleteTask
    load: type[SQL] = LoadTask
