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
# Modified   : Saturday January 7th 2023 09:32:43 am                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
from dataclasses import dataclass
from recsys.core.dal.sql.base import SQL, DDL, DML
from recsys.core.dal.dto import DTO
from recsys.core.entity.base import Entity
from recsys.core.entity.profile import Profile
# ================================================================================================ #
#                                        PROFILE                                                   #
# ================================================================================================ #


# ------------------------------------------------------------------------------------------------ #
#                                          DDL                                                     #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class CreateProfileTable(SQL):
    name: str = "profile"
    sql: str = """CREATE TABLE IF NOT EXISTS profile  (id MEDIUMINT PRIMARY KEY AUTO_INCREMENT, oid VARCHAR(255) AS (CONCAT('profile_', name, '_', mode)) NOT NULL, name VARCHAR(128) NOT NULL, description VARCHAR(255), mode VARCHAR(32), start DATETIME, end DATETIME, duration MEDIUMINT, user_cpu_time BIGINT, percent_cpu_used FLOAT, total_physical_memory BIGINT, physical_memory_available BIGINT, physical_memory_used BIGINT, percent_physical_memory_used FLOAT, active_memory_used BIGINT, disk_usage BIGINT, percent_disk_usage FLOAT, read_count BIGINT, write_count BIGINT, read_bytes BIGINT, write_bytes BIGINT, read_time FLOAT, write_time FLOAT, bytes_sent BIGINT, bytes_recv BIGINT, parent_id MEDIUMINT NOT NULL, created DATETIME, modified DATETIME, UNIQUE(name, mode));"""
    args: tuple = ()
    description: str = "Created the profile table."


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DropProfileTable(SQL):
    name: str = "profile"
    sql: str = """DROP TABLE IF EXISTS profile;"""
    args: tuple = ()
    description: str = "Dropped the profile table."


# ------------------------------------------------------------------------------------------------ #


@dataclass
class ProfileTableExists(SQL):
    name: str = "profile"
    sql: str = """SELECT COUNT(TABLE_NAME) FROM information_schema.TABLES WHERE TABLE_NAME = 'profile';"""
    args: tuple = ()
    description: str = "Checked existence of the profile table."


# ------------------------------------------------------------------------------------------------ #
@dataclass
class ProfileDDL(DDL):
    entity: type(Entity) = Profile
    create: SQL = CreateProfileTable()
    drop: SQL = DropProfileTable()
    exists: SQL = ProfileTableExists()


# ------------------------------------------------------------------------------------------------ #
#                                          DML                                                     #
# ------------------------------------------------------------------------------------------------ #


@dataclass
class InsertProfile(SQL):
    dto: DTO
    sql: str = """INSERT INTO profile (name, description, mode, start, end, duration, user_cpu_time, percent_cpu_used, total_physical_memory, physical_memory_available, physical_memory_used, percent_physical_memory_used, active_memory_used, disk_usage, percent_disk_usage, read_count, write_count, read_bytes, write_bytes, read_time, write_time, bytes_sent, bytes_recv, parent_id, created, modified) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (
            self.dto.name,
            self.dto.description,
            self.dto.mode,
            self.dto.start,
            self.dto.end,
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
            self.dto.parent_id,
            self.dto.created,
            self.dto.modified,
        )


# ------------------------------------------------------------------------------------------------ #


@dataclass
class UpdateProfile(SQL):
    dto: DTO
    sql: str = """UPDATE profile SET name = %s, description = %s, mode = %s, start = %s, end = %s, duration = %s, user_cpu_time = %s, percent_cpu_used = %s, total_physical_memory = %s, physical_memory_available = %s, physical_memory_used = %s, percent_physical_memory_used = %s, active_memory_used = %s, disk_usage = %s, percent_disk_usage = %s, read_count = %s, write_count = %s, read_bytes = %s, write_bytes = %s, read_time = %s, write_time = %s, bytes_sent = %s, bytes_recv = %s, parent_id = %s, created = %s, modified = %s WHERE id = %s;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (
            self.dto.name,
            self.dto.description,
            self.dto.mode,
            self.dto.start,
            self.dto.end,
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
            self.dto.parent_id,
            self.dto.created,
            self.dto.modified,
            self.dto.id,
        )


# ------------------------------------------------------------------------------------------------ #


@dataclass
class SelectProfile(SQL):
    id: int
    sql: str = """SELECT * FROM profile WHERE id = %s;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.id,)

# ------------------------------------------------------------------------------------------------ #


@dataclass
class SelectProfileByNameMode(SQL):
    name: str
    mode: str
    sql: str = """SELECT * FROM profile WHERE name = %s AND mode = %s;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.name, self.mode,)


# ------------------------------------------------------------------------------------------------ #


@dataclass
class SelectAllProfiles(SQL):
    sql: str = """SELECT * FROM profile;"""
    args: tuple = ()

# ------------------------------------------------------------------------------------------------ #


@dataclass
class ProfileExists(SQL):
    id: int
    sql: str = """SELECT EXISTS(SELECT 1 FROM profile WHERE id = %s LIMIT 1);"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.id,)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DeleteProfile(SQL):
    id: int
    sql: str = """DELETE FROM profile WHERE id = %s;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.id,)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class ProfileDML(DML):
    entity: type(Entity) = Profile
    insert: type(SQL) = InsertProfile
    update: type(SQL) = UpdateProfile
    select: type(SQL) = SelectProfile
    select_by_name_mode: type(SQL) = SelectProfileByNameMode
    select_all: type(SQL) = SelectAllProfiles
    exists: type(SQL) = ProfileExists
    delete: type(SQL) = DeleteProfile
