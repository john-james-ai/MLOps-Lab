#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/repo/uow.py                                                            #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday December 25th 2022 12:55:35 pm                                               #
# Modified   : Wednesday December 28th 2022 03:12:03 am                                            #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Unit of Work Module"""
from abc import ABC, abstractmethod

from dependency_injector.wiring import Provide, inject

from .base import Context
from recsys.containers import Recsys
from recsys.core.repo.dataset import DatasetRepo
from recsys.core.repo.job import JobRepo


# ------------------------------------------------------------------------------------------------ #
#                                 UNIT OF WORK ABC                                                 #
# ------------------------------------------------------------------------------------------------ #
class UnitOfWorkAbstract(ABC):

    @property
    @abstractmethod
    def dataset(self) -> DatasetRepo:
        """Returns an instantiated dataset repository."""

    @property
    @abstractmethod
    def job(self) -> JobRepo:
        """Returns an instantiated Job repository."""

    @abstractmethod
    def save(self):
        """Save changes."""

# ------------------------------------------------------------------------------------------------ #
#                                     UNIT OF WORK ABC                                             #
# ------------------------------------------------------------------------------------------------ #


class UnitOfWork(UnitOfWorkAbstract):

    @inject
    def __init__(self, context: Context = Provide[Recsys.dao]) -> None:
        self._context = context
        self._dataset = None
        self._job = None

    @property
    def dataset(self) -> DatasetRepo:
        if self._dataset is None:
            self._dataset = DatasetRepo(context=self._context)
        return self._dataset

    @property
    def job(self) -> JobRepo:
        if self._job is None:
            self._job = JobRepo(context=self._context)
        return self._job

    def save(self) -> None:
        self._context.save()
