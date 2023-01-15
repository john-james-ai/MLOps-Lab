#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/workflow/operator/data/loader.py                                       #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday December 30th 2022 10:04:01 am                                               #
# Modified   : Saturday January 14th 2023 09:17:44 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Loader Module."""
from typing import Any

from dependency_injector.wiring import Provide, inject
from dependency_injector import containers

from recsys.containers import Recsys
from recsys.core.repo.uow import UnitOfWork
from recsys.core.workflow.operator.base import Operator
from recsys.core.entity.datasource import DataSource


# ------------------------------------------------------------------------------------------------ #
#                                DATA SOURCE LOADER OPERATOR                                       #
# ------------------------------------------------------------------------------------------------ #
class DataSourceLoader(Operator):
    """Loads DataSource objects into the repository.

    Args:
        datasource (DataSource): The DataSource to load.

    """

    @inject
    def __init__(
        self, datasource: dict, factory: containers.DeclarativeContainer = Provide[Recsys.factory]
    ) -> None:
        super().__init__()
        self._datasource = datasource
        self._factory = factory()

    def execute(self, uow: UnitOfWork, data: Any = None) -> None:
        """Loads the DataSource into the Repository."""
        datasource = self._build_datasource()
        repo = uow.get_repo("datasource")
        repo.add(datasource)
        msg = f"Added DataSource {datasource.name} to repository."
        self._logger.debug(msg)

    def _build_datasource(self) -> DataSource:
        """Constructs the DataSource object."""
        datasource = self._factory.datasource(**self._datasource["datasource"])
        for urlconfig in self._datasource["urls"]:
            url = self._factory.datasource_url(**urlconfig)
            datasource.add_url(url)
        return datasource
