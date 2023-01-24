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
# Modified   : Sunday January 22nd 2023 02:42:24 pm                                                #
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
@dataclass(eq=False)
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
    task_oid: str
    created: datetime
    modified: datetime

    def __eq__(self, other) -> bool:
        if isinstance(other, ProfileDTO):
            return (
                self.task_oid == other.task_oid
                and self.start == other.start
                and self.end == other.end
            )
        else:
            return False


# ------------------------------------------------------------------------------------------------ #
#                               DATASET DATA TRANSFER OBJECT                                       #
# ------------------------------------------------------------------------------------------------ #
@dataclass(eq=False)
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
    dataset_oid: str
    created: datetime
    modified: datetime

    def __eq__(self, other) -> bool:
        if isinstance(other, DataFrameDTO):
            return (
                self.oid == other.oid
                and self.stage == other.stage
                and self.size == other.size
                and self.nrows == other.nrows
                and self.ncols == other.ncols
                and self.nulls == other.nulls
                and self.dataset_oid == other.dataset_oid
            )
        else:
            return False


# ------------------------------------------------------------------------------------------------ #
#                               DATASETS DATA TRANSFER OBJECT                                      #
# ------------------------------------------------------------------------------------------------ #
@dataclass(eq=False)
class DatasetDTO(DTO):
    id: int
    oid: str
    name: str
    description: str
    datasource_oid: str
    stage: str
    task_oid: str
    created: datetime
    modified: datetime

    def __eq__(self, other) -> bool:
        if isinstance(other, DatasetDTO):
            return (
                self.oid == other.oid
                and self.stage == other.stage
                and self.datasource_oid == other.datasource_oid
            )
        else:
            return False


# ------------------------------------------------------------------------------------------------ #
#                                   JOB DATA TRANSFER OBJECT                                       #
# ------------------------------------------------------------------------------------------------ #
@dataclass(eq=False)
class DAGDTO(DTO):
    id: int
    oid: str
    name: str
    description: str
    state: str
    created: datetime
    modified: datetime

    def __eq__(self, other) -> bool:
        if isinstance(other, DAGDTO):
            return self.oid == other.oid
        else:
            return False


# ------------------------------------------------------------------------------------------------ #
#                               TASK DATA TRANSFER OBJECT                                          #
# ------------------------------------------------------------------------------------------------ #
@dataclass(eq=False)
class TaskDTO(DTO):
    id: int
    oid: str
    name: str
    description: str
    state: str
    dag_oid: str
    created: datetime
    modified: datetime

    def __eq__(self, other) -> bool:
        if isinstance(other, TaskDTO):
            return self.oid == other.oid and self.dag_oid == other.dag_oid
        else:
            return False


# ------------------------------------------------------------------------------------------------ #
#                               FILE DATA TRANSFER OBJECT                                          #
# ------------------------------------------------------------------------------------------------ #
@dataclass(eq=False)
class FileDTO(DTO):
    id: int
    oid: str
    name: str
    description: str
    datasource_oid: str
    stage: str
    uri: str
    size: int
    task_oid: str
    created: datetime
    modified: datetime

    def __eq__(self, other) -> bool:
        if isinstance(other, FileDTO):
            return self.oid == other.oid and self.uri == other.uri and self.size == other.size
        else:
            return False


# ------------------------------------------------------------------------------------------------ #
#                               DATA SOURCE TRANSFER OBJECT                                        #
# ------------------------------------------------------------------------------------------------ #
@dataclass(eq=False)
class DataSourceDTO(DTO):
    id: int
    oid: str
    name: str
    description: str
    website: str
    created: datetime
    modified: datetime

    def __eq__(self, other) -> bool:
        if isinstance(other, DataSourceDTO):
            return self.oid == other.oid and self.website == other.website
        else:
            return False


# ------------------------------------------------------------------------------------------------ #
#                             DATA SOURCE URL TRANSFER OBJECT                                      #
# ------------------------------------------------------------------------------------------------ #
@dataclass(eq=False)
class DataSourceURLDTO(DTO):
    id: int
    oid: str
    name: str
    description: str
    url: str
    datasource_oid: str
    created: datetime
    modified: datetime

    def __eq__(self, other) -> bool:
        if isinstance(other, DataSourceURLDTO):
            return (
                self.oid == other.oid
                and self.url == other.url
                and self.datasource_oid == other.datasource_oid
            )
        else:
            return False


# ------------------------------------------------------------------------------------------------ #
#                                EVENT DATA TRANSFER OBJECT                                        #
# ------------------------------------------------------------------------------------------------ #
@dataclass(eq=False)
class EventDTO(DTO):
    id: int
    oid: str
    name: str
    description: str
    process_type: str
    process_oid: str
    parent_oid: str
    created: datetime
    modified: datetime

    def __eq__(self, other) -> bool:
        if isinstance(other, EventDTO):
            return (
                self.oid == other.oid
                and self.process_type == other.process_type
                and self.process_oid == other.process_oid
                and self.parent_oid == other.parent_oid
            )
        else:
            return False
