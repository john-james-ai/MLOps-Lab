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
# Created    : Wednesday December 7th 2022 09:19:05 pm                                             #
# Modified   : Friday December 9th 2022 10:21:30 am                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
from time import sleep
import psutil
import statistics
from datetime import datetime

from recsys.core import Service
from recsys.core.dal.dto import ProfileDTO

from .base import Entity
# ------------------------------------------------------------------------------------------------ #


class Profiler(Service):
    def __init__(self, task_id: int, name: str, interval: int = 0.1, description: str = None) -> None:
        self._profile = None
        self._task_id = task_id
        self._interval = interval
        super().__init__(name=name, description=description)

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

        self._active = False

    def profile(self) -> Entity:
        return self._profile

    def start(self) -> None:
        self._active = True
        self._started = datetime.now()
        while self._active:
            self._at_interval()
            sleep(self._interval)

    def end(self) -> None:
        self._ended = datetime.now()
        self._active = False
        self._compute()

    def snapshot(self) -> None:
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

    def _compute(self) -> None:
        """Computes the profile statistics."""
        time = TimeDTO(
            start=self._started,
            end=self._ended,
            duration=(self._ended - self._started).total_seconds(),
        )
        cpu = CPUDTO(
            user_cpu_time=self._user_cpu_time_list[-1] - self._user_cpu_time_list[0],
            percent_cpu_used=statistics.mean(self._percent_cpu_used_list),
        )
        memory = MemoryDTO(
            total_physical_memory=statistics.mean(self._total_physical_memory_list),
            physical_memory_available=statistics.mean(self._physical_memory_available_list),
            physical_memory_used=statistics.mean(self._physical_memory_used_list),
            percent_physical_memory_used=statistics.mean(self._percent_physical_memory_used_list),
            active_memory_used=statistics.mean(self._active_memory_used_list),
        )
        disk = DiskDTO(
            disk_usage=statistics.mean(self._disk_usage_list),
            percent_disk_usage=statistics.mean(self._percent_disk_usage_list),
            read_count=sum(self._read_count_list),
            write_count=sum(self._write_count_list),
            read_bytes=sum(self._read_bytes_list),
            write_bytes=sum(self._write_bytes_list),
            read_time=sum(self._read_time_list),
            write_time=sum(self._write_time_list),
        )
        network = NetworkDTO(
            bytes_sent=sum(self._bytes_sent_list),
            bytes_recv=sum(self._bytes_recv_list),
        )
        self._profile = ProfileDTO(time=time, cpu=cpu, memory=memory, disk=disk, network=network)
