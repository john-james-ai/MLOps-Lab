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
# Modified   : Thursday December 8th 2022 02:50:32 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
from dataclasses import dataclass
from recsys.core.dal.base import SQL, DDL, DML
from recsys.core.dal.dto import DTO


# ================================================================================================ #
#                                        DATASOURCE                                                #
# ================================================================================================ #


# ------------------------------------------------------------------------------------------------ #
#                                          DDL                                                     #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class CreateJobTable(SQL):
    name: str = "job"
    sql: str = """CREATE TABLE IF NOT EXISTS job (id INTEGER PRIMARY KEY, name TEXT NOT NULL, description TEXT, workspace TEXT NOT NULL, start TEXT, end TEXT, duration INTEGER DEFAULT (0), cpu_user_time INTEGER DEFAULT (0), cpu_percent REAL NOT NULL, physical_memory_total INTEGER DEFAULT (0), physical_memory_available INTEGER DEFAULT (0), physical_memory_used INTEGER DEFAULT (0), physical_memory_used_pct REAL NOT NULL, RAM_used INTEGER DEFAULT (0), RAM_used_pct REAL NOT NULL, disk_usage INTEGER DEFAULT (0), disk_usage_pct REAL NOT NULL, disk_read_count INTEGER DEFAULT (0), disk_write_count INTEGER DEFAULT (0), disk_read_bytes INTEGER DEFAULT (0), disk_write_bytes INTEGER DEFAULT (0), disk_read_time INTEGER DEFAULT (0), disk_write_time INTEGER DEFAULT (0), network_bytes_sent INTEGER DEFAULT (0), network_bytes_recv INTEGER DEFAULT (0));"""
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

    sql: str = """INSERT INTO job (name, description, workspace, start, end, duration, cpu_user_time, cpu_percent, physical_memory_total, physical_memory_available, physical_memory_used, physical_memory_used_pct, RAM_used, RAM_used_pct, disk_usage, disk_usage_pct, disk_read_count, disk_write_count, disk_read_bytes, disk_write_bytes, disk_read_time, disk_write_time, network_bytes_sent, network_bytes_recv) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (
            self.dto.name,
            self.dto.description,
            self.dto.workspace,
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
class UpdateJob(SQL):
    dto: DTO
    sql: str = """UPDATE job SET name = ?, description = ?, workspace = ?, start = ?, end = ?, duration = ?, cpu_user_time = ?, cpu_percent = ?, physical_memory_total = ?, physical_memory_available = ?, physical_memory_used = ?, physical_memory_used_pct = ?, RAM_used = ?, RAM_used_pct = ?, disk_usage = ?, disk_usage_pct = ?, disk_read_count = ?, disk_write_count = ?, disk_read_bytes = ?, disk_write_bytes = ?, disk_read_time = ?, disk_write_time = ?, network_bytes_sent = ?, network_bytes_recv = ?  WHERE id = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (
            self.dto.name,
            self.dto.description,
            self.dto.workspace,
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
class SelectJob(SQL):
    id: int
    sql: str = """SELECT * FROM job WHERE id = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.id,)


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
    select_all: type(SQL) = SelectAllJob
    exists: type(SQL) = JobExists
    delete: type(SQL) = DeleteJob
