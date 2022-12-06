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
# Modified   : Tuesday December 6th 2022 04:53:27 am                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import inspect
from datetime import datetime
import pandas as pd
from dataclasses import dataclass
from abc import ABC

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
        else:
            """Else nothing. What do you want?"""


# ------------------------------------------------------------------------------------------------ #
#                               DATASET DATA TRANSFER OBJECT                                       #
# ------------------------------------------------------------------------------------------------ #


@dataclass
class DatasetDTO(DTO):
    id: int
    name: str
    description: str
    source: str
    workspace: str
    stage: str
    version: int
    data: pd.DataFrame
    cost: int
    nrows: int
    ncols: int
    null_counts: int
    memory_size_mb: float
    filename: str
    filepath: str
    task_id: int
    creator: str
    created: datetime
    modified: datetime

    def __post_init__(self) -> None:
        stack = inspect.stack()
        try:
            self.creator = self.creator or stack[3][0].f_locals["self"].__class__.__name__
        except KeyError:
            self.creator = "Not Designated"


# ------------------------------------------------------------------------------------------------ #
#                               FILESET DATA TRANSFER OBJECT                                       #
# ------------------------------------------------------------------------------------------------ #


@dataclass
class FilesetDTO(DTO):
    id: int
    name: str
    description: str
    source: str
    filepath: str
    filesize: float
    task_id: int
    created: datetime
    modified: datetime


# ------------------------------------------------------------------------------------------------ #
#                                   JOB DATA TRANSFER OBJECT                                       #
# ------------------------------------------------------------------------------------------------ #


@dataclass
class JobDTO(DTO):
    id: int
    name: str
    description: str
    source: str
    workspace: str
    start: datetime
    end: datetime
    duration: int
    cpu_user_time: int
    cpu_percent: float
    physical_memory_total: int
    physical_memory_available: int
    physical_memory_used: int
    physical_memory_used_pct: float
    RAM_used: int
    RAM_used_pct: float
    disk_usage: int
    disk_usage_pct: float
    disk_read_count: int
    disk_write_count: int
    disk_read_bytes: int
    disk_write_bytes: int
    disk_read_time: int
    disk_write_time: int
    network_bytes_sent: int
    network_bytes_recv: int


# ------------------------------------------------------------------------------------------------ #
#                               OPERATOR DATA TRANSFER OBJECT                                      #
# ------------------------------------------------------------------------------------------------ #


@dataclass
class OperatorDTO(DTO):
    id: int
    name: str
    description: str
    module: str
    classname: str
    filepath: str


# ------------------------------------------------------------------------------------------------ #
#                               DATASOURCE DATA TRANSFER OBJECT                                    #
# ------------------------------------------------------------------------------------------------ #


@dataclass
class DataSourceDTO(DTO):
    id: int
    kind: str
    name: str
    description: str
    website: str
    link: str
    filepath: str


# ------------------------------------------------------------------------------------------------ #
#                               TASK DATA TRANSFER OBJECT                                    #
# ------------------------------------------------------------------------------------------------ #


@dataclass
class TaskDTO(DTO):
    id: int
    job_id: int
    name: str
    description: str
    operator: str
    module: str
    input_kind: str
    input_id: int
    output_kind: str
    output_id: int
    start: datetime
    end: datetime
    duration: int
    cpu_user_time: int
    cpu_percent: float
    physical_memory_total: int
    physical_memory_available: int
    physical_memory_used: int
    physical_memory_used_pct: float
    RAM_used: int
    RAM_used_pct: float
    disk_usage: int
    disk_usage_pct: float
    disk_read_count: int
    disk_write_count: int
    disk_read_bytes: int
    disk_write_bytes: int
    disk_read_time: int
    disk_write_time: int
    network_bytes_sent: int
    network_bytes_recv: int
