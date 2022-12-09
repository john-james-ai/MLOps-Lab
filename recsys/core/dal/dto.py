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
# Modified   : Thursday December 8th 2022 05:24:55 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
from datetime import datetime
from dataclasses import dataclass

from .base import DTO


# ------------------------------------------------------------------------------------------------ #
#                               PROFILE DATA TRANSFER OBJECT                                       #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class TimeDTO(DTO):
    start: datetime
    end: datetime
    duration: int


# ------------------------------------------------------------------------------------------------ #
@dataclass
class CPUDTO(DTO):
    user_cpu_time: int
    percent_cpu_used: float


# ------------------------------------------------------------------------------------------------ #
@dataclass
class MemoryDTO(DTO):
    total_physical_memory: int
    physical_memory_available: int
    physical_memory_used: int
    percent_physical_memory_used: float
    active_memory_used: int


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DiskDTO(DTO):
    disk_usage: int
    percent_disk_usage: float
    read_count: int
    write_count: int
    read_bytes: int
    write_bytes: int
    read_time: int
    write_time: int


# ------------------------------------------------------------------------------------------------ #
@dataclass
class NetworkDTO(DTO):
    bytes_sent: int
    bytes_recv: int


# ------------------------------------------------------------------------------------------------ #
@dataclass
class ProfileDTO(DTO):
    id: int
    job_task_id: int
    time: TimeDTO
    cpu: CPUDTO
    memory: MemoryDTO
    disk: DiskDTO
    network: NetworkDTO


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
    cost: int
    nrows: int
    ncols: int
    null_counts: int
    memory_size_mb: float
    filepath: str
    task_id: int
    created: datetime
    modified: datetime


# ------------------------------------------------------------------------------------------------ #
#                               FILESET DATA TRANSFER OBJECT                                       #
# ------------------------------------------------------------------------------------------------ #


@dataclass
class FilesetDTO(DTO):
    id: int
    name: str
    description: str
    source: str
    uri: str
    filesize: int
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
    workspace: str
    profile: ProfileDTO


# ------------------------------------------------------------------------------------------------ #
#                               DATASOURCE DATA TRANSFER OBJECT                                    #
# ------------------------------------------------------------------------------------------------ #

@dataclass
class DataSourceDTO(DTO):
    id: int
    name: str
    publisher: str
    description: str
    website: str
    url: str


# ------------------------------------------------------------------------------------------------ #
#                               TASK DATA TRANSFER OBJECT                                          #
# ------------------------------------------------------------------------------------------------ #


@dataclass
class TaskDTO(DTO):
    id: int
    job_id: int
    name: str
    description: str
    workspace: str
    operator: str
    module: str
    input_kind: str
    input_id: int
    output_kind: str
    output_id: int
    profile: ProfileDTO
