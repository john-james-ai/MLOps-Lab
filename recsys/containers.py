#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /containers.py                                                                      #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday November 25th 2022 03:18:36 pm                                               #
# Modified   : Friday December 2nd 2022 06:27:27 am                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #

import logging
import sqlite3
from dependency_injector import containers, providers

from recsys.services.io import IOService
from recsys.data.database import Database
from recsys.core.dal.dto import DatasetDTO


# ------------------------------------------------------------------------------------------------ #
#                                         CORE                                                     #
# ------------------------------------------------------------------------------------------------ #


class Core(containers.DeclarativeContainer):

    config = providers.Configuration()

    logging = providers.Resource(
        logging.config.dictConfig,
        config=config.logging,
    )


# ------------------------------------------------------------------------------------------------ #
#                                         CORE                                                     #
# ------------------------------------------------------------------------------------------------ #
class DataLayer(containers.DeclarativeContainer):

    config = providers.Configuration()

    database = providers.Singleton(sqlite3.connect, config.data.database.sqlite.location)


class Services(containers.DeclarativeContainer):

    config = providers.Configuration()

    wiring_config = containers.WiringConfiguration(
        modules=[
            "recsys.core.services.repository",
            "recsys.core.workflow.pipeline",
            # "recsys.methods.neighborhood.data.process",
        ]
    )

    ENV = get_env()

    config = providers.Configuration()

    config.from_dict(
        {
            "archive_directory": ARCHIVE_DIRS["data"][ENV],
            "repo_directory": REPO_DIRS["data"][ENV],
            "db_location": DB_LOCATIONS["data"][ENV],
            "repo_file_format": REPO_FILE_FORMAT,
        }
    )

    io = providers.Factory(IOService)

    db = providers.Factory(Database, config.db_location)

    dataset_dto = providers.Factory(DatasetDTO, database=db)


container = Container()
