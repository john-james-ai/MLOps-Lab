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
# Modified   : Saturday January 7th 2023 12:47:47 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Main Module."""
from dependency_injector.wiring import Provide, inject

from recsys.containers import Recsys
from recsys.core.dal.dba import DBA

# ------------------------------------------------------------------------------------------------ #


@inject
def build_file_table(file_table: DBA = Provide[Recsys.table.file]) -> None:
    file_table.reset()
    assert file_table.exists()


@inject
def build_datasource_table(datasource_table: DBA = Provide[Recsys.table.datasource]) -> None:
    datasource_table.reset()
    assert datasource_table.exists()


@inject
def build_datasource_url_table(datasource_url_table: DBA = Provide[Recsys.table.datasource_url]) -> None:
    datasource_url_table.reset()
    assert datasource_url_table.exists()


@inject
def build_dataframe_table(dataframe_table: DBA = Provide[Recsys.table.dataframe]) -> None:
    dataframe_table.reset()
    assert dataframe_table.exists()


@inject
def build_dataset_table(dataset_table: DBA = Provide[Recsys.table.dataset]) -> None:
    dataset_table.reset()
    assert dataset_table.exists()


@inject
def build_job_table(job_table: DBA = Provide[Recsys.table.job]) -> None:
    job_table.reset()
    assert job_table.exists()


@inject
def build_task_table(task_table: DBA = Provide[Recsys.table.task]) -> None:
    task_table.reset()
    assert task_table.exists()


@inject
def build_profile_table(task_table: DBA = Provide[Recsys.table.profile]) -> None:
    task_table.reset()
    assert task_table.exists()


def build_tables():
    build_file_table()
    build_datasource_table()
    build_datasource_url_table()
    build_dataframe_table()
    build_dataset_table()
    build_job_table()
    build_task_table()
    build_profile_table()


# ------------------------------------------------------------------------------------------------ #
def wireup():
    recsys = Recsys()
    recsys.core.init_resources()
    recsys.wire(modules=[__name__, "recsys.core.repo.context"])


def main():
    wireup()
    build_tables()


if __name__ == "__main__":
    main()
