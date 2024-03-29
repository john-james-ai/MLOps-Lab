#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Enter Project Name in Workspace Settings                                            #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /mlops_lab/core/dal/sql/profile.py                                                  #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : Enter URL in Workspace Settings                                                     #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday December 8th 2022 02:06:04 pm                                              #
# Modified   : Tuesday January 24th 2023 08:13:42 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import os
import dotenv

from dataclasses import dataclass
from mlops_lab.core.dal.sql.base import SQL, DDL, DML
from mlops_lab.core.dal.dto import DTO
from mlops_lab.core.entity.base import Entity
from mlops_lab.core.workflow.profile import Profile

# ================================================================================================ #
#                                        PROFILE                                                   #
# ================================================================================================ #


# ------------------------------------------------------------------------------------------------ #
#                                          DDL                                                     #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class CreateProfileTable(SQL):
    name: str = "profile"
    sql: str = """CREATE TABLE IF NOT EXISTS profile  (id MEDIUMINT PRIMARY KEY AUTO_INCREMENT, oid VARCHAR(255) NOT NULL, name VARCHAR(128) NOT NULL, description VARCHAR(255), start DATETIME, end DATETIME, duration MEDIUMINT, user_cpu_time BIGINT, percent_cpu_used FLOAT, total_physical_memory BIGINT, physical_memory_available BIGINT, physical_memory_used BIGINT, percent_physical_memory_used FLOAT, active_memory_used BIGINT, disk_usage BIGINT, percent_disk_usage FLOAT, read_count BIGINT, write_count BIGINT, read_bytes BIGINT, write_bytes BIGINT, read_time FLOAT, write_time FLOAT, bytes_sent BIGINT, bytes_recv BIGINT, task_oid VARCHAR(128) NOT NULL, created DATETIME DEFAULT CURRENT_TIMESTAMP, modified DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, UNIQUE(name));"""
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
    sql: str = None
    args: tuple = ()
    description: str = "Checked existence of profile table."

    def __post_init__(self) -> None:
        dotenv.load_dotenv()
        mode = os.getenv("MODE")
        self.sql = f"""SELECT COUNT(TABLE_NAME) FROM information_schema.TABLES WHERE TABLE_SCHEMA LIKE 'mlops_lab_{mode}' AND TABLE_NAME = 'profile';"""


# ------------------------------------------------------------------------------------------------ #
@dataclass
class ProfileDDL(DDL):
    entity: type[Entity] = Profile
    create: SQL = CreateProfileTable()
    drop: SQL = DropProfileTable()
    exists: SQL = ProfileTableExists()


# ------------------------------------------------------------------------------------------------ #
#                                          DML                                                     #
# ------------------------------------------------------------------------------------------------ #


@dataclass
class InsertProfile(SQL):
    dto: DTO
    sql: str = """INSERT INTO profile (oid, name, description, start, end, duration, user_cpu_time, percent_cpu_used, total_physical_memory, physical_memory_available, physical_memory_used, percent_physical_memory_used, active_memory_used, disk_usage, percent_disk_usage, read_count, write_count, read_bytes, write_bytes, read_time, write_time, bytes_sent, bytes_recv, task_oid) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (
            self.dto.oid,
            self.dto.name,
            self.dto.description,
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
            self.dto.task_oid,
        )


# ------------------------------------------------------------------------------------------------ #


@dataclass
class UpdateProfile(SQL):
    dto: DTO
    sql: str = """UPDATE profile SET oid = %s, name = %s, description = %s, start = %s, end = %s, duration = %s, user_cpu_time = %s, percent_cpu_used = %s, total_physical_memory = %s, physical_memory_available = %s, physical_memory_used = %s, percent_physical_memory_used = %s, active_memory_used = %s, disk_usage = %s, percent_disk_usage = %s, read_count = %s, write_count = %s, read_bytes = %s, write_bytes = %s, read_time = %s, write_time = %s, bytes_sent = %s, bytes_recv = %s, task_oid = %s WHERE id = %s;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (
            self.dto.oid,
            self.dto.name,
            self.dto.description,
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
            self.dto.task_oid,
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
class SelectProfileByName(SQL):
    name: str
    sql: str = """SELECT * FROM profile WHERE name = %s;"""
    args: tuple = ()

    def __post_init__(self) -> None:
        self.args = (self.name,)


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
class LoadProfile(SQL):
    filename: str
    tablename: str = "profile"
    sql: str = None
    args: tuple = ()

    def __post_init__(self) -> None:
        self.sql = f"""LOAD DATA LOCAL INFILE '{self.filename}' INTO TABLE {self.tablename} FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\r\n' IGNORE 1 ROWS;"""


# ------------------------------------------------------------------------------------------------ #
@dataclass
class ProfileDML(DML):
    entity: type[Entity] = Profile
    insert: type[SQL] = InsertProfile
    update: type[SQL] = UpdateProfile
    select: type[SQL] = SelectProfile
    select_by_name: type[SQL] = SelectProfileByName
    select_all: type[SQL] = SelectAllProfiles
    exists: type[SQL] = ProfileExists
    delete: type[SQL] = DeleteProfile
    load: type[SQL] = LoadProfile
