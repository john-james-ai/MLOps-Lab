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
# Modified   : Sunday December 11th 2022 06:18:23 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
from datetime import datetime
from dataclasses import dataclass
from typing import List

from .base import DTO


# ------------------------------------------------------------------------------------------------ #
#                               PROFILE DATA TRANSFER OBJECT                                       #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class ProfileDTO(DTO):
    id: int
    name: str
    description: str
    started: datetime
    ended: datetime
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
    task_id: int
    created: datetime
    modified: datetime


# ------------------------------------------------------------------------------------------------ #
#                               DATASET DATA TRANSFER OBJECT                                       #
# ------------------------------------------------------------------------------------------------ #


@dataclass
class DatasetDTO(DTO):
    id: int
    name: str
    description: str
    datasource: str
    workspace: str
    stage: str
    filepath: str
    size: int
    nrows: int
    ncols: int
    nulls: int
    pct_nulls: float
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
    datasource: str
    workspace: str
    stage: str
    uri: str
    task_id: int
    created: datetime
    modified: datetime
    filesize: int = None


# ------------------------------------------------------------------------------------------------ #
#                                   JOB DATA TRANSFER OBJECT                                       #
# ------------------------------------------------------------------------------------------------ #


@dataclass
class JobDTO(DTO):
    id: int
    name: str
    description: str
    workspace: str
    started: datetime
    ended: datetime
    duration: int
    tasks_completed: int
    created: datetime
    modified: datetime


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
    created: datetime
    modified: datetime
    filesets: List[FilesetDTO] = None


# ------------------------------------------------------------------------------------------------ #
#                               TASK DATA TRANSFER OBJECT                                          #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class TaskDTO(DTO):
    id: int
    name: str
    description: str
    workspace: str
    operator: str
    module: str
    job_id: int
    profile_id: int
    created: datetime
    modified: datetime


# ------------------------------------------------------------------------------------------------ #
#                               TASK RESOURCE DATA TRANSFER OBJECT                                 #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class TaskResourceDTO(DTO):
    id: int
    name: str
    description: str
    task_id: int
    resource_kind: str
    resource_id: int
    resource_context: str
    created: datetime
    modified: datetime
