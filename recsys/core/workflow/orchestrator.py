#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/services/orchestrator.py                                               #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday January 12th 2023 09:09:55 pm                                              #
# Modified   : Friday January 13th 2023 11:57:33 pm                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2023 John James                                                                 #
# ================================================================================================ #
"""Orchestrator Module"""
from typing import Any

from recsys.core.services.base import Service
from recsys.core.repo.uow import UnitOfWork
from recsys.core.entity.job import Job
from recsys import STATES


# ------------------------------------------------------------------------------------------------ #
class Orchestrator(Service):
    def __init__(self, uow: UnitOfWork) -> None:
        super().__init__()
        self._job = None
        self._uow = uow
        self._data = None

    @property
    def job(self) -> Job:
        return self._job

    @job.setter
    def job(self, job: Job) -> None:
        self._job = job
        self._load()

    @property
    def data(self) -> Any:
        return self._data

    @data.setter
    def data(self, data: Any) -> None:
        self._data = data

    def reset(self) -> None:
        self._job = None

    def run(self) -> None:
        """Runs the jobs in the orchestrator in the order in which they were added."""
        with self._uow as uow:
            try:
                self._start(uow=uow)
                self._data = self._job.run(uow=uow, data=self._data)
                self._end(uow=uow)
            except Exception:  # pragma: no cover
                self._failed(uow=uow)

        return self._data

    def _load(self) -> None:
        self._job.state = STATES[1]
        repo = self._uow.get_repo("job")
        self._job = repo.add(self._job)
        msg = f"Job {self._job.name} has been loaded into the orchestrator."
        self._logger.info(msg)

    def _start(self, uow=UnitOfWork) -> None:
        self._job.state = STATES[2]
        repo = uow.get_repo("job")
        repo.update(self._job)
        msg = f"Job {self._job.name} has started."
        self._logger.info(msg)

    def _end(self, uow=UnitOfWork) -> None:
        self._job.state = STATES[4]
        repo = uow.get_repo("job")
        repo.update(self._job)
        msg = f"Job {self._job.name} has ended."
        self._logger.info(msg)

    def _failed(self, uow=UnitOfWork) -> None:
        self._job.state = STATES[3]
        repo = uow.get_repo("job")
        repo.update(self._job)
        msg = f"Job {self._job.name} failed."
        self._logger.info(msg)
