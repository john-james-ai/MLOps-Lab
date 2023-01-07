#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/setup/reset_db.py                                                           #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday January 7th 2023 11:56:00 am                                               #
# Modified   : Saturday January 7th 2023 12:49:36 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2023 John James                                                                 #
# ================================================================================================ #
"""Drops all databases and tables, and brings state to zero."""
from dependency_injector.wiring import Provide, inject

from recsys.core.dal.dba import DBA, ODBA
from recsys.containers import Recsys


# ------------------------------------------------------------------------------------------------ #
@inject
def remove_rdb(dba=Provide[Recsys.dba.database]):
    dba.drop()
    assert not dba.exists()


# ------------------------------------------------------------------------------------------------ #
@inject
def remove_odb(dba=Provide[Recsys.dba.object]):
    dba.drop()
    assert not dba.exists()


# ------------------------------------------------------------------------------------------------ #
@inject
def build_file_table(file_table: DBA = Provide[Recsys.dba.file]) -> None:
    file_table.create()
    assert file_table.exists()


# ------------------------------------------------------------------------------------------------ #
@inject
def build_datasource_table(datasource_table: DBA = Provide[Recsys.dba.datasource]) -> None:
    datasource_table.create()
    assert datasource_table.exists()


# ------------------------------------------------------------------------------------------------ #
@inject
def build_datasource_url_table(datasource_url_table: DBA = Provide[Recsys.dba.datasource_url]) -> None:
    datasource_url_table.create()
    assert datasource_url_table.exists()


# ------------------------------------------------------------------------------------------------ #
@inject
def build_dataframe_table(dataframe_table: DBA = Provide[Recsys.dba.dataframe]) -> None:
    dataframe_table.create()
    assert dataframe_table.exists()


# ------------------------------------------------------------------------------------------------ #
@inject
def build_dataset_table(dataset_table: DBA = Provide[Recsys.dba.dataset]) -> None:
    dataset_table.create()
    assert dataset_table.exists()


# ------------------------------------------------------------------------------------------------ #
@inject
def build_job_table(job_table: DBA = Provide[Recsys.dba.job]) -> None:
    job_table.create()
    assert job_table.exists()


# ------------------------------------------------------------------------------------------------ #
@inject
def build_task_table(task_table: DBA = Provide[Recsys.dba.task]) -> None:
    task_table.create()
    assert task_table.exists()


# ------------------------------------------------------------------------------------------------ #
@inject
def build_profile_table(task_table: DBA = Provide[Recsys.dba.profile]) -> None:
    task_table.create()
    assert task_table.exists()


# ------------------------------------------------------------------------------------------------ #
@inject
def build_object_db(odb: ODBA = Provide[Recsys.dba.object]) -> None:
    odb.create()
    assert odb.exists()


# ------------------------------------------------------------------------------------------------ #
def remove():
    remove_rdb()
    remove_odb()


# ------------------------------------------------------------------------------------------------ #
def rebuild():
    build_file_table()
    build_datasource_table()
    build_datasource_url_table()
    build_dataframe_table()
    build_dataset_table()
    build_job_table()
    build_task_table()
    build_profile_table()
    build_object_db()


# ------------------------------------------------------------------------------------------------ #
def wireup():
    recsys = Recsys()
    recsys.core.init_resources()
    recsys.wire(modules=[__name__])


# ------------------------------------------------------------------------------------------------ #
def main():
    wireup()
    remove()
    rebuild()


# ------------------------------------------------------------------------------------------------ #
if __name__ == "__main__":
    main()
