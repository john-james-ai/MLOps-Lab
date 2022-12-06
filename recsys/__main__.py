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
# Modified   : Tuesday December 6th 2022 06:32:20 am                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Main Module."""
from dependency_injector.wiring import Provide, inject

from load_tables import load_tables
from containers import Recsys
from recsys.core.dal.ddo import TableService


@inject
def build_dataset_table(dataset_table: TableService = Provide[Recsys.dal.dataset_table]) -> None:
    dataset_table.create()
    assert dataset_table.exists()


@inject
def build_fileset_table(fileset_table: TableService = Provide[Recsys.dal.fileset_table]) -> None:
    fileset_table.create()
    assert fileset_table.exists()


@inject
def build_job_table(job_table: TableService = Provide[Recsys.dal.job_table]) -> None:
    job_table.create()
    assert job_table.exists()


@inject
def build_operator_table(
    operator_table: TableService = Provide[Recsys.dal.operator_table],
) -> None:
    operator_table.create()
    assert operator_table.exists()


@inject
def build_datasource_table(
    datasource_table: TableService = Provide[Recsys.dal.datasource_table],
) -> None:
    datasource_table.create()
    assert datasource_table.exists()


@inject
def build_task_table(task_table: TableService = Provide[Recsys.dal.task_table]) -> None:
    task_table.create()
    assert task_table.exists()


def build_tables():
    build_dataset_table()
    build_fileset_table()
    build_job_table()
    build_operator_table()
    build_datasource_table()
    build_task_table()


def main():
    wireup()
    build_tables()
    load_tables()


def wireup():
    recsys = Recsys()
    recsys.core.init_resources()
    recsys.wire(modules=[__name__, build_tables, load_tables])


if __name__ == "__main__":  # pragma: no cover
    main()
