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
# Modified   : Saturday January 21st 2023 05:04:40 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Main Module."""
from dependency_injector.wiring import Provide, inject
from dependency_injector.providers import Factory

from recsys.core.dal.dba import DBA, ODBA
from recsys.container import Recsys


# ------------------------------------------------------------------------------------------------ #
@inject
def reset_edb(dba=Provide[Recsys.dba.events_database]):
    dba.reset()
    assert dba.exists()


# ------------------------------------------------------------------------------------------------ #
@inject
def reset_rdb(dba=Provide[Recsys.dba.recsys_database]):
    dba.reset()
    assert dba.exists()


# ------------------------------------------------------------------------------------------------ #
@inject
def reset_odb(dba=Provide[Recsys.dba.object]):
    dba.reset()
    assert dba.exists()


# ------------------------------------------------------------------------------------------------ #
@inject
def build_event_table(event_table: Factory[DBA] = Provide[Recsys.dba.event]) -> None:
    event_table.create()
    assert event_table.exists()


# ------------------------------------------------------------------------------------------------ #
@inject
def build_file_table(file_table: Factory[DBA] = Provide[Recsys.dba.file]) -> None:
    file_table.create()
    assert file_table.exists()


# ------------------------------------------------------------------------------------------------ #
@inject
def build_datasource_table(datasource_table: Factory[DBA] = Provide[Recsys.dba.datasource]) -> None:
    datasource_table.create()
    assert datasource_table.exists()


# ------------------------------------------------------------------------------------------------ #
@inject
def build_datasource_url_table(
    datasource_url_table: Factory[DBA] = Provide[Recsys.dba.datasource_url],
) -> None:
    datasource_url_table.create()
    assert datasource_url_table.exists()


# ------------------------------------------------------------------------------------------------ #
@inject
def build_dataframe_table(dataframe_table: Factory[DBA] = Provide[Recsys.dba.dataframe]) -> None:
    dataframe_table.create()
    assert dataframe_table.exists()


# ------------------------------------------------------------------------------------------------ #
@inject
def build_dataset_table(dataset_table: Factory[DBA] = Provide[Recsys.dba.dataset]) -> None:
    dataset_table.create()
    assert dataset_table.exists()


# ------------------------------------------------------------------------------------------------ #
@inject
def build_dag_table(dag_table: Factory[DBA] = Provide[Recsys.dba.dag]) -> None:
    dag_table.create()
    assert dag_table.exists()


# ------------------------------------------------------------------------------------------------ #
@inject
def build_task_table(task_table: Factory[DBA] = Provide[Recsys.dba.task]) -> None:
    task_table.create()
    assert task_table.exists()


# ------------------------------------------------------------------------------------------------ #
@inject
def build_profile_table(profile_table: Factory[DBA] = Provide[Recsys.dba.profile]) -> None:
    profile_table.create()
    assert profile_table.exists()


# ------------------------------------------------------------------------------------------------ #
@inject
def build_object_db(odb: Factory[ODBA] = Provide[Recsys.dba.object]) -> None:
    odb.create()
    assert odb.exists()


# ------------------------------------------------------------------------------------------------ #
def reset():
    reset_edb()
    reset_rdb()
    reset_odb()


# ------------------------------------------------------------------------------------------------ #
def rebuild():
    build_event_table()
    build_profile_table()
    build_file_table()
    build_datasource_table()
    build_datasource_url_table()
    build_dataframe_table()
    build_dataset_table()
    build_dag_table()
    build_task_table()
    build_object_db()


# ------------------------------------------------------------------------------------------------ #
def wireup():
    recsys = Recsys()
    recsys.core.init_resources()
    recsys.wire(modules=[__name__])


# ------------------------------------------------------------------------------------------------ #
def main():
    wireup()
    reset()
    rebuild()


# ------------------------------------------------------------------------------------------------ #
if __name__ == "__main__":
    main()
