#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /container.py                                                                       #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday November 25th 2022 03:18:36 pm                                               #
# Modified   : Tuesday November 29th 2022 09:19:48 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import os
from dotenv import load_dotenv
from dependency_injector import containers, providers

from recsys.core.services.io import IOService
from recsys.core.dal.database import Database
from recsys.core.dal.registry import DatasetRegistry
from recsys.core.dal.repo import DatasetRepo
from recsys.config.data import REPO_DIRS, REPO_FILE_FORMAT, DB_LOCATIONS, ARCHIVE_DIRS

# ------------------------------------------------------------------------------------------------ #
#                                       CONTAINER                                                  #
# ------------------------------------------------------------------------------------------------ #


def get_env():
    load_dotenv()
    return os.getenv("ENV")


class Container(containers.DeclarativeContainer):

    wiring_config = containers.WiringConfiguration(
        modules=[
            "recsys.core.services.decorator",
            "recsys.core.workflow.pipeline",
            # "recsys.recommenders.collabfilter.data.process",
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

    registry = providers.Factory(DatasetRegistry, database=db)

    repo = providers.Factory(
        DatasetRepo,
        repo_directory=config.repo_directory,
        archive_directory=config.archive_directory,
        io=IOService,
        registry=registry,
        file_format=config.repo_file_format,
    )


container = Container()
