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
# Modified   : Sunday December 11th 2022 02:30:32 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import psutil
import statistics
from datetime import datetime

from .base import Entity, DTO
from recsys.core.dal.dto import ProfileDTO
# ------------------------------------------------------------------------------------------------ #


class Profile(Entity):
    """Encapsulates cpu, memory, disk, network, and time resources consumed by tasks and jobs.

    Args:
        name (str): Name of job or task being profiled
        description (str): Description of job or task being profiled.
    """

    def __init__(self, name: str, task_id: int, description: str = None) -> None:
        super().__init__(name=name, description=description)
        self._task_id = task_id
        self._started = None
        self._ended = None
        self._duration = None
        self._user_cpu_time = None
        self._percent_cpu_used = None
        self._total_physical_memory = None
        self._physical_memory_available = None
        self._physical_memory_used = None
        self._percent_physical_memory_used = None
        self._active_memory_used = None
        self._disk_usage = None
        self._percent_disk_usage = None
        self._read_count = None
        self._write_count = None
        self._read_bytes = None
        self._write_bytes = None
        self._read_time = None
        self._write_time = None
        self._bytes_sent = None
        self._bytes_recv = None

        self._user_cpu_time_list = []
        self._percent_cpu_used_list = []
        self._total_physical_memory_list = []
        self._physical_memory_available_list = []
        self._physical_memory_used_list = []
        self._percent_physical_memory_used_list = []
        self._active_memory_used_list = []
        self._disk_usage_list = []
        self._percent_disk_usage_list = []
        self._read_count_list = []
        self._write_count_list = []
        self._read_bytes_list = []
        self._write_bytes_list = []
        self._read_time_list = []
        self._write_time_list = []
        self._bytes_sent_list = []
        self._bytes_recv_list = []

    # -------------------------------------------------------------------------------------------- #

    @property
    def time(self) -> dict:
        return {"started": self._started, "ended": self._ended, "duration": self._duration}

    @property
    def cpu(self) -> dict:
        return {"user_cpu_time": self._user_cpu_time, "percent_cpu_used": self._percent_cpu_used}

    @property
    def memory(self) -> dict:
        return {"total_physical_memory": self._total_physical_memory,
                "physical_memory_available": self._physical_memory_available,
                "physical_memory_used": self._physical_memory_used,
                "percent_physical_memory_used": self._percent_physical_memory_used,
                "active_memory_used": self._active_memory_used
                }

    @property
    def disk(self) -> dict:
        return {"disk_usage": self._disk_usage,
                "percent_disk_usage": self._percent_disk_usage,
                "read_count": self._read_count,
                "write_count": self._write_count,
                "read_bytes": self._read_bytes,
                "write_bytes": self._write_bytes,
                "read_time": self._read_time,
                "write_time": self._write_time
                }

    @property
    def network(self) -> dict:
        return {"bytes_sent": self._bytes_sent,
                "bytes_recv": self._bytes_recv
                }

    # -------------------------------------------------------------------------------------------- #
    def start(self) -> None:
        """Sets start time."""
        self._started = datetime.now()

    # -------------------------------------------------------------------------------------------- #
    def end(self) -> None:
        """Sets end time and computes duration."""
        self._ended = datetime.now()
        self._duration = (self._ended - self._started).total_seconds()

    # -------------------------------------------------------------------------------------------- #
    def snapshot(self) -> None:
        """Takes snapshot of cpu, memory, network and disk usage and stores in a list."""
        # Grab the statistics
        cpu_time = psutil.cpu_times().user
        cpu_pct = psutil.cpu_percent()
        vmem = psutil.virtual_memory()
        disk_usage = psutil.disk_usage('/')
        disk_io = psutil.disk_io_counters()
        network = psutil.net_io_counters()

        # Append stats to respective lists
        self._user_cpu_time_list.append(cpu_time)
        self._percent_cpu_used_list.append(cpu_pct)
        self._total_physical_memory_list.append(vmem.total)
        self._physical_memory_available_list.append(vmem.available)
        self._physical_memory_used_list.append(vmem.used)
        self._percent_physical_memory_used_list.append((vmem.total - vmem.available) / vmem.total * 100)
        self._active_memory_used_list.append(vmem.active)
        self._disk_usage_list.append(disk_usage.used)
        self._percent_disk_usage_list.append(disk_usage.percent)
        self._read_count_list.append(disk_io.read_count)
        self._write_count_list.append(disk_io.write_count)
        self._read_bytes_list.append(disk_io.read_bytes)
        self._write_bytes_list.append(disk_io.write_bytes)
        self._read_time_list.append(disk_io.read_time)
        self._write_time_list.append(disk_io.write_time)
        self._bytes_sent_list.append(network.bytes_sent)
        self._bytes_recv_list.append(network.bytes_recv)

    # -------------------------------------------------------------------------------------------- #
    def compute(self) -> None:
        """Computes usage statistics."""
        # CPU
        self._user_cpu_time = self._user_cpu_time_list[-1] - self._user_cpu_time_list[0]
        self._percent_cpu_used = statistics.mean(self._percent_cpu_used_list)

        # Memory
        self._total_physical_memory = statistics.mean(self._total_physical_memory_list)
        self._physical_memory_available = statistics.mean(self._physical_memory_available_list)
        self._physical_memory_used = statistics.mean(self._physical_memory_used_list)
        self._percent_physical_memory_used = statistics.mean(self._percent_physical_memory_used_list)
        self._active_memory_used = statistics.mean(self._active_memory_used_list)

        # Disk
        self._disk_usage = statistics.mean(self._disk_usage_list)
        self._percent_disk_usage = statistics.mean(self._percent_disk_usage_list)
        self._read_count = sum(self._read_count_list)
        self._write_count = sum(self._write_count_list)
        self._read_bytes = sum(self._read_bytes_list)
        self._write_bytes = sum(self._write_bytes_list)
        self._read_time = sum(self._read_time_list)
        self._write_time = sum(self._write_time_list)

        # Network
        self._bytes_sent = sum(self._bytes_sent_list)
        self._bytes_recv = sum(self._bytes_recv_list)

    # -------------------------------------------------------------------------------------------- #
    def as_dto(self) -> ProfileDTO:
        return ProfileDTO(
            id=self._id,
            name=self._name,
            description=self._description,
            start=self._start,
            end=self._end,
            duration=self._duration,
            user_cpu_time=self._user_cpu_time,
            percent_cpu_used=self._percent_cpu_used,
            total_physical_memory=self._total_physical_memory,
            physical_memory_available=self._physical_memory_available,
            physical_memory_used=self._physical_memory_used,
            percent_physical_memory_used=self._percent_physical_memory_used,
            active_memory_used=self._active_memory_used,
            disk_usage=self._disk_usage,
            percent_disk_usage=self._percent_disk_usage,
            read_count=self._read_count,
            write_count=self._write_count,
            read_bytes=self._read_bytes,
            write_bytes=self._write_bytes,
            read_time=self._read_time,
            write_time=self._write_time,
            bytes_sent=self._bytes_sent,
            bytes_recv=self._bytes_recv,
            task_id=self._task_id,
            created=self._created,
            modified=self._modified,
        )

    # -------------------------------------------------------------------------------------------- #
    def _from_dto(self, dto: DTO) -> None:
        super().__init__(name=dto.name, description=dto.description)
        self._id = dto.id
        self._start = dto.start
        self._end = dto.end
        self._duration = dto.duration
        self._user_cpu_time = dto.user_cpu_time
        self._percent_cpu_used = dto.percent_cpu_used
        self._total_physical_memory = dto.total_physical_memory
        self._physical_memory_available = dto.physical_memory_available
        self._physical_memory_used = dto.physical_memory_used
        self._percent_physical_memory_used = dto.percent_physical_memory_used
        self._active_memory_used = dto.active_memory_used
        self._disk_usage = dto.disk_usage
        self._percent_disk_usage = dto.percent_disk_usage
        self._read_count = dto.read_count
        self._write_count = dto.write_count
        self._read_bytes = dto.read_bytes
        self._write_bytes = dto.write_bytes
        self._read_time = dto.read_time
        self._write_time = dto.write_time
        self._bytes_sent = dto.bytes_sent
        self._bytes_recv = dto.bytes_recv
        self._task_id = dto.task_id
        self._created = dto.created
        self._modified = dto.modified

        self._validate()
