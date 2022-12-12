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
# Modified   : Monday December 12th 2022 01:31:53 am                                               #
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
    sql: str = """CREATE TABLE IF NOT EXISTS profile  (id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE, description TEXT, started timestamp, ended timestamp, duration INTEGER DEFAULT 0, user_cpu_time INTEGER DEFAULT 0, percent_cpu_used REAL NOT NULL, total_physical_memory INTEGER DEFAULT 0, physical_memory_available INTEGER DEFAULT 0, physical_memory_used INTEGER DEFAULT 0, percent_physical_memory_used REAL NOT NULL, active_memory_used INTEGER DEFAULT 0, disk_usage INTEGER DEFAULT 0, percent_disk_usage REAL NOT NULL, read_count INTEGER DEFAULT 0, write_count INTEGER DEFAULT 0, read_bytes INTEGER DEFAULT 0, write_bytes INTEGER DEFAULT 0, read_time INTEGER DEFAULT 0, write_time INTEGER DEFAULT 0, bytes_sent INTEGER DEFAULT 0, bytes_recv INTEGER DEFAULT 0, task_id INTEGER NOT NULL, created timestamp, modified timestamp);"""
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
    sql: str = """INSERT INTO profile (name, description, started, ended, duration, user_cpu_time, percent_cpu_used, total_physical_memory, physical_memory_available, physical_memory_used, percent_physical_memory_used, active_memory_used, disk_usage, percent_disk_usage, read_count, write_count, read_bytes, write_bytes, read_time, write_time, bytes_sent, bytes_recv, task_id, created, modified) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (
            self.dto.name,
            self.dto.description,
            self.dto.started,
            self.dto.ended,
            self.dto.duration,
            self.dto.user_cpu_time,
            self.dto.percent_cpu_used,
            self.dto.total_physical_memory,
            self.dto.physical_memory_available,
            self.dto.physical_memory_used,
            self.dto.percent_physical_memory_used,
            self.dto.active_memory_used,
            self.dto.disk_usage,
            self.dto.percent_disk_usage,
            self.dto.read_count,
            self.dto.write_count,
            self.dto.read_bytes,
            self.dto.write_bytes,
            self.dto.read_time,
            self.dto.write_time,
            self.dto.bytes_sent,
            self.dto.bytes_recv,
            self.dto.task_id,
            self.dto.created,
            self.dto.modified,
        )


# ------------------------------------------------------------------------------------------------ #


@dataclass
class UpdateProfile(SQL):
    dto: DTO
    sql: str = """UPDATE profile SET name = ?, description = ?, started = ?, ended = ?, duration = ?, user_cpu_time = ?, percent_cpu_used = ?, total_physical_memory = ?, physical_memory_available = ?, physical_memory_used = ?, percent_physical_memory_used = ?, active_memory_used = ?, disk_usage = ?, percent_disk_usage = ?, read_count = ?, write_count = ?, read_bytes = ?, write_bytes = ?, read_time = ?, write_time = ?, bytes_sent = ?, bytes_recv = ?, task_id = ?, created = ?, modified = ?  WHERE id = ?;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (
            self.dto.name,
            self.dto.description,
            self.dto.started,
            self.dto.ended,
            self.dto.duration,
            self.dto.user_cpu_time,
            self.dto.percent_cpu_used,
            self.dto.total_physical_memory,
            self.dto.physical_memory_available,
            self.dto.physical_memory_used,
            self.dto.percent_physical_memory_used,
            self.dto.active_memory_used,
            self.dto.disk_usage,
            self.dto.percent_disk_usage,
            self.dto.read_count,
            self.dto.write_count,
            self.dto.read_bytes,
            self.dto.write_bytes,
            self.dto.read_time,
            self.dto.write_time,
            self.dto.bytes_sent,
            self.dto.bytes_recv,
            self.dto.task_id,
            self.dto.created,
            self.dto.modified,
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
