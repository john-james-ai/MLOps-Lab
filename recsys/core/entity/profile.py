#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/entity/profile.py                                                      #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday December 9th 2022 10:54:47 pm                                                #
# Modified   : Saturday January 7th 2023 09:31:20 am                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
from dataclasses import dataclass
from datetime import datetime

from recsys.core.entity.base import Entity
from recsys.core.dal.dto import ProfileDTO


# ------------------------------------------------------------------------------------------------ #
@dataclass
class Profile(Entity):
    """Profile Entity and DTO are nearly identical. Entity has Null defaults."""
    id: int = None
    oid: str = None
    name: str = None
    description: str = None
    mode: str = None
    start: datetime = None
    end: datetime = None
    duration: int = None
    user_cpu_time: int = None
    percent_cpu_used: float = None
    total_physical_memory: int = None
    physical_memory_available: int = None
    physical_memory_used: int = None
    percent_physical_memory_used: float = None
    active_memory_used: int = None
    disk_usage: int = None
    percent_disk_usage: float = None
    read_count: int = None
    write_count: int = None
    read_bytes: int = None
    write_bytes: int = None
    read_time: int = None
    write_time: int = None
    bytes_sent: int = None
    bytes_recv: int = None
    parent_id: int = None
    created: datetime = None
    modified: datetime = None

    def as_dto(self) -> ProfileDTO:
        return ProfileDTO(
            id=self.id,
            oid=self.oid,
            name=self.name,
            description=self.description,
            mode=self.mode,
            start=self.start,
            end=self.end,
            duration=self.duration,
            user_cpu_time=self.user_cpu_time,
            percent_cpu_used=self.percent_cpu_used,
            total_physical_memory=self.total_physical_memory,
            physical_memory_available=self.physical_memory_available,
            physical_memory_used=self.physical_memory_used,
            percent_physical_memory_used=self.percent_physical_memory_used,
            active_memory_used=self.active_memory_used,
            disk_usage=self.disk_usage,
            percent_disk_usage=self.percent_disk_usage,
            read_count=self.read_count,
            write_count=self.write_count,
            read_bytes=self.read_bytes,
            write_bytes=self.write_bytes,
            read_time=self.read_time,
            write_time=self.write_time,
            bytes_sent=self.bytes_sent,
            bytes_recv=self.bytes_recv,
            parent_id=self.parent_id,
            created=self.created,
            modified=self.modified,
        )
