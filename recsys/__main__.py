#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/__main__.py                                                                 #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday December 3rd 2022 02:32:23 pm                                              #
# Modified   : Friday December 16th 2022 06:36:01 am                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Main Module."""
from dependency_injector.wiring import Provide, inject

from recsys.containers import Recsys
from recsys.core.dal.ddo import TableService


@inject
def build_dataset_table(dataset_table: TableService = Provide[Recsys.table.dataset]) -> None:
    dataset_table.create()
    assert dataset_table.exists()


@inject
def build_job_table(job_table: TableService = Provide[Recsys.table.job]) -> None:
    job_table.create()
    assert job_table.exists()


@inject
def build_task_table(task_table: TableService = Provide[Recsys.table.task]) -> None:
    task_table.create()
    assert task_table.exists()


@inject
def build_profile_table(task_table: TableService = Provide[Recsys.table.profile]) -> None:
    task_table.create()
    assert task_table.exists()


def build_tables():
    build_dataset_table()
    build_job_table()
    build_task_table()
    build_profile_table()


def wireup():
    recsys = Recsys()
    recsys.core.init_resources()
    recsys.wire(modules=[__name__])


def main():
    wireup()
    build_tables()


if __name__ == "__main__":
    main()
