#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Enter Project Name in Workspace Settings                                            #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /mlops_lab/core/workflow/operator/data/loader.py                                    #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : Enter URL in Workspace Settings                                                     #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday December 30th 2022 10:04:01 am                                               #
# Modified   : Tuesday January 24th 2023 08:13:48 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Loader Module."""
from typing import Any
from types import SimpleNamespace

from dependency_injector.wiring import Provide, inject
from dependency_injector import containers

from mlops_lab.container import mlops_lab
from mlops_lab.core.repo.uow import UnitOfWork
from mlops_lab.core.workflow.operator.base import Operator
from mlops_lab.core.entity.datasource import DataSource


# ------------------------------------------------------------------------------------------------ #
#                                DATA SOURCE LOADER OPERATOR                                       #
# ------------------------------------------------------------------------------------------------ #
class DataSourceLoader(Operator):
    """Loads DataSource objects into the repository.

    Args:
        config (dict): Dictionary with a single key: 'datasource'. The values describe the
            name, description, website, and urls for the datasource.
        factory (Container): A container of entity factories. (Should be called an
            industry, but i thought factory.entity provided better semantics.)

    """

    @inject
    def __init__(
        self, config: dict, factory: containers.DeclarativeContainer = Provide[mlops_lab.factory]
    ) -> None:
        super().__init__()
        datasource = SimpleNamespace(**config["datasource"])
        setattr(self, "datasource", datasource)
        self._datasource = config["datasource"]
        self._factory = factory

    def execute(self, uow: UnitOfWork, data: Any = None) -> None:
        """Loads the DataSource into the Repository."""
        datasource = self._build_datasource()
        repo = uow.get_repo("datasource")
        repo.add(datasource)
        msg = f"Added DataSource {datasource.name} to repository."
        self._logger.debug(msg)

    def _build_datasource(self) -> DataSource:
        """Constructs the DataSource object."""
        datasource = self._factory.datasource()(**self._datasource)
        for urlconfig in self._datasource.urls:
            url = self._factory.datasource_url()(**urlconfig)
            datasource.add_url(url)
        return datasource
