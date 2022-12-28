#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/data/movielens25m/etl.py                                                    #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday December 16th 2022 08:05:00 pm                                               #
# Modified   : Sunday December 25th 2022 10:26:13 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
from dependency_injector.wiring import Provide, inject

from recsys.core.workflow.job import Job
from recsys.core.repo.base import Repo
from recsys.containers import Recsys
from .container import MovieLens25M
# ------------------------------------------------------------------------------------------------ #


class Movielens25mETL(Job):
    """ETL for MovieLens25M DataFrame"""
# ------------------------------------------------------------------------------------------------ #


class Movielens25mETLBuilder:

    @inject
    def __init__(self, dataframe_repo: Repo = Provide[Recsys.dataframe_repo],
                 job_repo: Repo = Provide[Recsys.job_repo],
                 task_repo: Repo = Provide[Recsys.task_repo]) -> None:
        self._dataframe_repo = dataframe_repo
        self._job_repo = job_repo
        self._task_repo = task_repo
        self._job = None

    @property
    def job(self) -> Job:
        return self._job

    @inject
    def build_job(self, job: Job = Provide[MovieLens25M.job.job]) -> None:
        self._job = self._job_repo.add(job)
