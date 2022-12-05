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
# Created    : Saturday December 3rd 2022 11:21:14 am                                              #
# Modified   : Saturday December 3rd 2022 06:21:58 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import logging.config

from dependency_injector import containers, providers


# ------------------------------------------------------------------------------------------------ #
class LoggingContainer(containers.DeclarativeContainer):  # pragma: no cover

    config = providers.Configuration(yaml_files=["tests/config.yml"])

    logging = providers.Resource(
        logging.config.dictConfig,
        config=config.logging,
    )


logging_container = LoggingContainer()
logging_container.init_resources()
