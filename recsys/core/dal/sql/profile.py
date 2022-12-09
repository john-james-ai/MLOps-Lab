#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/dal/sql/profile.py                                                     #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday December 8th 2022 02:06:04 pm                                              #
# Modified   : Thursday December 8th 2022 02:59:12 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
from dataclasses import dataclass
from recsys.core.dal.base import SQL, DDL, DML
from recsys.core.dal.dto import DTO

# ================================================================================================ #
#                                        PROFILE                                                   #
# ================================================================================================ #


# ------------------------------------------------------------------------------------------------ #
#                                          DDL                                                     #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class CreateProfileTable(SQL):
    name: str = "profile"
    sql: str = """CREATE TABLE IF NOT EXISTS profile  (id INTEGER PRIMARY KEY, job_task_id INTEGER DEFAULT (0), start timestamp, end timestamp, duration INTEGER DEFAULT (0), user_cpu_time INTEGER DEFAULT (0), percent_cpu_used REAL NOT NULL, total_physical_memory INTEGER DEFAULT (0), physical_memory_available INTEGER DEFAULT (0), physical_memory_used INTEGER DEFAULT (0), percent_physical_memory_used REAL NOT NULL, active_memory_used INTEGER DEFAULT (0), disk_usage INTEGER DEFAULT (0), percent_disk_usage REAL NOT NULL, read_count INTEGER DEFAULT (0), write_count INTEGER DEFAULT (0), read_bytes INTEGER DEFAULT (0), write_bytes INTEGER DEFAULT (0), read_time INTEGER DEFAULT (0), write_time INTEGER DEFAULT (0), bytes_sent INTEGER DEFAULT (0), bytes_recv INTEGER DEFAULT (0));"""
    args: tuple = ()


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DropProfileTable(SQL):
    name: str = "profile"
    sql: str = """DROP TABLE IF EXISTS profile;"""
    args: tuple = ()


# ------------------------------------------------------------------------------------------------ #


@dataclass
class ProfileTableExists(SQL):
    name: str = "profile"
    sql: str = """SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.name,)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class ProfileDDL(DDL):
    create: SQL = CreateProfileTable()
    drop: SQL = DropProfileTable()
    exists: SQL = ProfileTableExists()


# ------------------------------------------------------------------------------------------------ #
#                                          DML                                                     #
# ------------------------------------------------------------------------------------------------ #


@dataclass
class InsertProfile(SQL):
    dto: DTO
    sql: str = """INSERT INTO profile (id, job_task_id, start, end, duration, user_cpu_time, percent_cpu_used, total_physical_memory, physical_memory_available, physical_memory_used, percent_physical_memory_used, active_memory_used, disk_usage, percent_disk_usage, read_count, write_count, read_bytes, write_bytes, read_time, write_time, bytes_sent, bytes_recv) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (
            self.dto.id,
            self.dto.job_task_id,
            self.dto.time.start,
            self.dto.time.end,
            self.dto.time.duration,
            self.dto.cpu.user_cpu_time,
            self.dto.cpu.percent_cpu_used,
            self.dto.memory.total_physical_memory,
            self.dto.memory.physical_memory_available,
            self.dto.memory.physical_memory_used,
            self.dto.memory.percent_physical_memory_used,
            self.dto.memory.active_memory_used,
            self.dto.disk.disk_usage,
            self.dto.disk.percent_disk_usage,
            self.dto.disk.read_count,
            self.dto.disk.write_count,
            self.dto.disk.read_bytes,
            self.dto.disk.write_bytes,
            self.dto.disk.read_time,
            self.dto.disk.write_time,
            self.dto.network.bytes_sent,
            self.dto.network.bytes_recv,

        )


# ------------------------------------------------------------------------------------------------ #


@dataclass
class UpdateProfile(SQL):
    dto: DTO
    sql: str = """UPDATE profile SET job_task_id = ?, start = ?, end = ?, duration = ?, user_cpu_time = ?, percent_cpu_used = ?, total_physical_memory = ?, physical_memory_available = ?, physical_memory_used = ?, percent_physical_memory_used = ?, active_memory_used = ?, disk_usage = ?, percent_disk_usage = ?, read_count = ?, write_count = ?, read_bytes = ?, write_bytes = ?, read_time = ?, write_time = ?, bytes_sent = ?, bytes_recv = ? WHERE id = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (
            self.dto.job_task_id,
            self.dto.time.start,
            self.dto.time.end,
            self.dto.time.duration,
            self.dto.cpu.user_cpu_time,
            self.dto.cpu.percent_cpu_used,
            self.dto.memory.total_physical_memory,
            self.dto.memory.physical_memory_available,
            self.dto.memory.physical_memory_used,
            self.dto.memory.percent_physical_memory_used,
            self.dto.memory.active_memory_used,
            self.dto.disk.disk_usage,
            self.dto.disk.percent_disk_usage,
            self.dto.disk.read_count,
            self.dto.disk.write_count,
            self.dto.disk.read_bytes,
            self.dto.disk.write_bytes,
            self.dto.disk.read_time,
            self.dto.disk.write_time,
            self.dto.network.bytes_sent,
            self.dto.network.bytes_recv,
            self.dto.id,

        )


# ------------------------------------------------------------------------------------------------ #


@dataclass
class SelectProfile(SQL):
    id: int
    sql: str = """SELECT * FROM profile WHERE id = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.id,)


# ------------------------------------------------------------------------------------------------ #


@dataclass
class SelectAllProfiles(SQL):
    sql: str = """SELECT * FROM profile;"""
    args: tuple = ()


# ------------------------------------------------------------------------------------------------ #


@dataclass
class ProfileExists(SQL):
    id: int
    sql: str = """SELECT COUNT(*) FROM profile WHERE id = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.id,)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DeleteProfile(SQL):
    id: int
    sql: str = """DELETE FROM profile WHERE id = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.id,)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class ProfileDML(DML):
    insert: type(SQL) = InsertProfile
    update: type(SQL) = UpdateProfile
    select: type(SQL) = SelectProfile
    select_all: type(SQL) = SelectAllProfiles
    exists: type(SQL) = ProfileExists
    delete: type(SQL) = DeleteProfile
