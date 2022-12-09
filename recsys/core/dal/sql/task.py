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
# Modified   : Wednesday December 7th 2022 08:09:18 pm                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
from dataclasses import dataclass
from recsys.core.dal.base import SQL, DDL, DML
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
    sql: str = """CREATE TABLE IF NOT EXISTS task (id INTEGER PRIMARY KEY, job_id INTEGER DEFAULT (0), name TEXT NOT NULL, description TEXT NOT NULL, workspace TEXT NOT NULL, operator TEXT NOT NULL, module TEXT NOT NULL, input_kind TEXT NOT NULL, input_id INTEGER DEFAULT (0), output_kind TEXT NOT NULL, output_id INTEGER DEFAULT (0), start TEXT, end TEXT, duration INTEGER DEFAULT (0), cpu_user_time INTEGER DEFAULT (0), cpu_percent REAL NOT NULL, physical_memory_total INTEGER DEFAULT (0), physical_memory_available INTEGER DEFAULT (0), physical_memory_used INTEGER DEFAULT (0), physical_memory_used_pct REAL NOT NULL, RAM_used INTEGER DEFAULT (0), RAM_used_pct REAL NOT NULL, disk_usage INTEGER DEFAULT (0), disk_usage_pct REAL NOT NULL, disk_read_count INTEGER DEFAULT (0), disk_write_count INTEGER DEFAULT (0), disk_read_bytes INTEGER DEFAULT (0), disk_write_bytes INTEGER DEFAULT (0), disk_read_time INTEGER DEFAULT (0), disk_write_time INTEGER DEFAULT (0), network_bytes_sent INTEGER DEFAULT (0), network_bytes_recv INTEGER DEFAULT (0));"""
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

    sql: str = """INSERT INTO task (job_id, name, description, workspace, operator, module, input_kind, input_id, output_kind, output_id, start, end, duration, cpu_user_time, cpu_percent, physical_memory_total, physical_memory_available, physical_memory_used, physical_memory_used_pct, RAM_used, RAM_used_pct, disk_usage, disk_usage_pct, disk_read_count, disk_write_count, disk_read_bytes, disk_write_bytes, disk_read_time, disk_write_time, network_bytes_sent, network_bytes_recv) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);"""


def __post_init__(self) -> None:
    self.args = (
        self.dto.job_id,
        self.dto.name,
        self.dto.description,
        self.dto.workspace,
        self.dto.operator,
        self.dto.module,
        self.dto.input_kind,
        self.dto.input_id,
        self.dto.output_kind,
        self.dto.output_id,
        self.dto.start,
        self.dto.end,
        self.dto.duration,
        self.dto.cpu_user_time,
        self.dto.cpu_percent,
        self.dto.physical_memory_total,
        self.dto.physical_memory_available,
        self.dto.physical_memory_used,
        self.dto.physical_memory_used_pct,
        self.dto.RAM_used,
        self.dto.RAM_used_pct,
        self.dto.disk_usage,
        self.dto.disk_usage_pct,
        self.dto.disk_read_count,
        self.dto.disk_write_count,
        self.dto.disk_read_bytes,
        self.dto.disk_write_bytes,
        self.dto.disk_read_time,
        self.dto.disk_write_time,
        self.dto.network_bytes_sent,
        self.dto.network_bytes_recv,
    )


# ------------------------------------------------------------------------------------------------ #


@dataclass
class UpdateTask(SQL):
    dto: DTO
    sql: str = """UPDATE task SET job_id = ?, name = ?, description = ?, workspace = ?, operator = ?, module = ?, input_kind = ?, input_id = ?, output_kind = ?, output_id = ?, start = ?, end = ?, duration = ?, cpu_user_time = ?, cpu_percent = ?, physical_memory_total = ?, physical_memory_available = ?, physical_memory_used = ?, physical_memory_used_pct = ?, RAM_used = ?, RAM_used_pct = ?, disk_usage = ?, disk_usage_pct = ?, disk_read_count = ?, disk_write_count = ?, disk_read_bytes = ?, disk_write_bytes = ?, disk_read_time = ?, disk_write_time = ?, network_bytes_sent = ?, network_bytes_recv = ? WHERE id = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (
            self.dto.job_id,
            self.dto.name,
            self.dto.description,
            self.dto.workspace,
            self.dto.operator,
            self.dto.module,
            self.dto.input_kind,
            self.dto.input_id,
            self.dto.output_kind,
            self.dto.output_id,
            self.dto.start,
            self.dto.end,
            self.dto.duration,
            self.dto.cpu_user_time,
            self.dto.cpu_percent,
            self.dto.physical_memory_total,
            self.dto.physical_memory_available,
            self.dto.physical_memory_used,
            self.dto.physical_memory_used_pct,
            self.dto.RAM_used,
            self.dto.RAM_used_pct,
            self.dto.disk_usage,
            self.dto.disk_usage_pct,
            self.dto.disk_read_count,
            self.dto.disk_write_count,
            self.dto.disk_read_bytes,
            self.dto.disk_write_bytes,
            self.dto.disk_read_time,
            self.dto.disk_write_time,
            self.dto.network_bytes_sent,
            self.dto.network_bytes_recv,
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
    select_all: type(SQL) = SelectAllTasks
    exists: type(SQL) = TaskExists
    delete: type(SQL) = DeleteTask
