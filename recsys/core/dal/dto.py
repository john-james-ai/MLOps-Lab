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
# Modified   : Sunday December 25th 2022 12:12:59 am                                               #
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
    oid: str
    name: str
    description: str
    datasource: str
    mode: str
    stage: str
    size: int
    nrows: int
    ncols: int
    nulls: int
    pct_nulls: float
    task_id: int
    parent_id: int
    created: datetime
    modified: datetime


# ------------------------------------------------------------------------------------------------ #
#                               DATASETS DATA TRANSFER OBJECT                                      #
# ------------------------------------------------------------------------------------------------ #

@dataclass
class DatasetsDTO(DTO):
    id: int
    oid: str
    name: str
    description: str
    datasource: str
    mode: str
    stage: str
    task_id: int
    parent_id: int
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
    mode: str
    started: datetime
    ended: datetime
    duration: float
    state: str
    created: datetime
    modified: datetime


# ------------------------------------------------------------------------------------------------ #
#                               TASK DATA TRANSFER OBJECT                                          #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class TaskDTO(DTO):
    id: int
    name: str
    description: str
    mode: str
    stage: str
    job_id: int
    started: datetime
    ended: datetime
    duration: float
    created: datetime
    modified: datetime
