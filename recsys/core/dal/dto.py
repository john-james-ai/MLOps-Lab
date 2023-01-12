#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/dal/dto.py                                                             #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday December 4th 2022 01:09:22 pm                                                #
# Modified   : Wednesday January 11th 2023 07:00:56 pm                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
from abc import ABC
from datetime import datetime
from dataclasses import dataclass

from recsys import IMMUTABLE_TYPES, SEQUENCE_TYPES


# ------------------------------------------------------------------------------------------------ #
#                              DATA TRANSFER OBJECT ABC                                            #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class DTO(ABC):  # pragma: no cover
    """Data Transfer Object"""

    def as_dict(self) -> dict:
        """Returns a dictionary representation of the the Config object."""
        return {k: self._export_config(v) for k, v in self.__dict__.items()}

    @classmethod
    def _export_config(cls, v):
        """Returns v with Configs converted to dicts, recursively."""
        if isinstance(v, IMMUTABLE_TYPES):
            return v
        elif isinstance(v, SEQUENCE_TYPES):
            return type(v)(map(cls._export_config, v))
        elif isinstance(v, datetime):
            return v.strftime("%H:%M:%S on %m/%d/%Y")
        elif isinstance(v, dict):
            return v
        elif hasattr(v, "as_dict"):
            return v.as_dict()
        else:
            """Else nothing. What do you want?"""


# ------------------------------------------------------------------------------------------------ #
#                               PROFILE DATA TRANSFER OBJECT                                       #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class ProfileDTO(DTO):
    id: int
    oid: str
    name: str
    description: str
    start: datetime
    end: datetime
    duration: int
    user_cpu_time: int
    percent_cpu_used: float
    total_physical_memory: int
    physical_memory_available: int
    physical_memory_used: int
    percent_physical_memory_used: float
    active_memory_used: int
    disk_usage: int
    percent_disk_usage: float
    read_count: int
    write_count: int
    read_bytes: int
    write_bytes: int
    read_time: int
    write_time: int
    bytes_sent: int
    bytes_recv: int
    parent_id: int
    created: datetime
    modified: datetime


# ------------------------------------------------------------------------------------------------ #
#                               DATASET DATA TRANSFER OBJECT                                       #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class DataFrameDTO(DTO):
    id: int
    oid: str
    name: str
    description: str
    stage: str
    size: int
    nrows: int
    ncols: int
    nulls: int
    pct_nulls: float
    parent_id: int
    created: datetime
    modified: datetime


# ------------------------------------------------------------------------------------------------ #
#                               DATASETS DATA TRANSFER OBJECT                                      #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class DatasetDTO(DTO):
    id: int
    oid: str
    name: str
    description: str
    datasource_id: int
    stage: str
    task_id: int
    created: datetime
    modified: datetime


# ------------------------------------------------------------------------------------------------ #
#                                   JOB DATA TRANSFER OBJECT                                       #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class JobDTO(DTO):
    id: int
    oid: str
    name: str
    description: str
    state: str
    created: datetime
    modified: datetime


# ------------------------------------------------------------------------------------------------ #
#                               TASK DATA TRANSFER OBJECT                                          #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class TaskDTO(DTO):
    id: int
    oid: str
    name: str
    description: str
    state: str
    parent_id: int
    created: datetime
    modified: datetime


# ------------------------------------------------------------------------------------------------ #
#                               FILE DATA TRANSFER OBJECT                                          #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class FileDTO(DTO):
    id: int
    oid: str
    name: str
    description: str
    datasource_id: int
    stage: str
    uri: str
    size: int
    task_id: int
    created: datetime
    modified: datetime


# ------------------------------------------------------------------------------------------------ #
#                               DATA SOURCE TRANSFER OBJECT                                        #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class DataSourceDTO(DTO):
    id: int
    oid: str
    name: str
    description: str
    website: str
    created: datetime
    modified: datetime


# ------------------------------------------------------------------------------------------------ #
#                             DATA SOURCE URL TRANSFER OBJECT                                      #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class DataSourceURLDTO(DTO):
    id: int
    oid: str
    name: str
    description: str
    url: str
    parent_id: int
    created: datetime
    modified: datetime
