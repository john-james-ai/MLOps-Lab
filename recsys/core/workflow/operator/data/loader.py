#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/workflow/operators/data/loader.py                                      #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday December 30th 2022 10:04:01 am                                               #
# Modified   : Saturday January 14th 2023 04:15:06 am                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Loader Module."""
from typing import Any

from recsys.core.entity.datasource import DataSource
from recsys.core.repo.uow import UnitOfWork
from ..base import Operator


# ------------------------------------------------------------------------------------------------ #
#                                DATA SOURCE LOADER OPERATOR                                       #
# ------------------------------------------------------------------------------------------------ #
class DataSourceLoader(Operator):
    """Loads DataSource objects into the repository.

    Args:
        datasource (DataSource): The DataSource to load.

    """

    def __init__(self, datasource: DataSource) -> None:
        super().__init__()
        self._datasource = datasource

    def execute(self, uow: UnitOfWork, data: Any = None) -> None:
        """Loads the DataSource into the Repository."""
        repo = uow.get_repo("datasource")
        repo.add(self._datasource)
        msg = f"Added DataSource {self._datasource.name} to repository."
        self._logger.debug(msg)
