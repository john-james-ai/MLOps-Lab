#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/load_tables.py                                                              #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday December 6th 2022 04:21:13 am                                               #
# Modified   : Tuesday December 6th 2022 06:34:40 am                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
from dependency_injector.wiring import Provide, inject

from containers import Recsys


@inject
def load_tables(
    config=Provide[Recsys.config],
    table=Provide[Recsys.dal.datasource_table],
    dao=Provide[Recsys.dal.source_dao],
) -> None:
    sources = config["data_sources"]
    for source in sources:
        print(source)
