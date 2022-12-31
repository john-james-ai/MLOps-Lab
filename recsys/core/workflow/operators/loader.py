#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/workflow/operators/loader.py                                           #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday December 30th 2022 10:04:01 am                                               #
# Modified   : Friday December 30th 2022 08:10:31 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Loader Module."""
from types import SimpleNamespace

from recsys.core.entity.file import File
from recsys.core.entity.datasource import DataSource, DataSourceURL
from recsys.core.workflow.base import Operator


# ------------------------------------------------------------------------------------------------ #
#                                    FILE LOADER OPERATOR                                          #
# ------------------------------------------------------------------------------------------------ #
class FileLoader(Operator):
    """Loads entities into the repositories.

    Args:
        task_params (dict): Name, description and mode parameters for the Task object.
        input_params (dict): The parameters specifying the input directory File object.
        output_params (dict): The output File or Files parameters.

    """

    def __init__(self, task_params: dict, input_params: dict, output_params: dict) -> None:

        self._task_params = SimpleNamespace(**task_params)
        self._input_params = SimpleNamespace(**input_params)
        self._output_params = SimpleNamespace(**output_params)

        super().__init__(name=self._task_params.name, mode=self._task_params.mode, description=self._task_params.description)
        self._entities = self._output_params.entities

    def execute(self, *args, **kwargs) -> None:
        """Creates the task, and the File objects, and loads them into the File repository."""
        task = self.setup()
        for file in self._entities:
            file.task_id = task.id
            self._put_file(file)
        self._teardown(task)

    def _put_file(self, file: File) -> None:
        self._uow.file.create(file)


# ------------------------------------------------------------------------------------------------ #
#                                DATA SOURCE LOADER OPERATOR                                       #
# ------------------------------------------------------------------------------------------------ #
class DataSourceLoader(Operator):
    """Loads DataSource objects into the repository.

    Args:
        task_params (dict): Name, description and mode parameters for the Task object.
        input_params (dict): The parameters specifying the input directory File object.
        output_params (dict): The output File or Files parameters.

    """

    def __init__(self, task_params: dict, output_params: dict) -> None:

        self._task_params = SimpleNamespace(**task_params)
        self._output_params = SimpleNamespace(**output_params)

        super().__init__(name=self._task_params.name, mode=self._task_params.mode, description=self._task_params.description)

    def execute(self) -> None:
        """Loads the File objects into the Repository."""

        task = self.setup()
        datasource = self._build_datasource()
        self._put_datasource(datasource)
        self._teardown(task)

    def _build_datasource(self) -> DataSource:
        datasource = DataSource(
            name=self._output_params.name,
            description=self._output_params.description,
            website=self._output_params.website,
        )
        for url in self._output_params.urls:
            ds_url = DataSourceURL(
                name=url.name,
                url=url.url,
                parent=datasource,
                description=url.description,
            )
            datasource.add_url(ds_url)
        return datasource

    def _put_datasource(self, datasource: DataSource) -> None:
        """Stores a datasource in the DataSource repository."""
        self._uow.datasource.create(datasource)
        ds_urls = datasource.urls
        for ds_url in ds_urls.values():
            self._uow.datasource_url.create(ds_url)
