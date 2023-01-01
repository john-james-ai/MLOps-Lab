#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/workflow/operators/download.py                                         #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday December 30th 2022 10:02:44 am                                               #
# Modified   : Saturday December 31st 2022 05:49:43 pm                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Download Operator Module"""
from types import SimpleNamespace

from .base import Operator
from atelier.operator.download import Downloader


# ================================================================================================ #
#                                    DOWNLOAD OPERATOR                                             #
# ================================================================================================ #
class Download(Operator):
    """Downloads and if compressed, extracts data from a website.

    Args:
        task_params (dict): Name, description and mode parameters for the Task object.
        operator_params (dict): Operator name, module and operator parameters.
        input_params (dict): The parameters specifying the input DataSource
        output_params (dict): The output File and uri parameters.

    """

    def __init__(self, task_params: dict, input_params: dict, output_params: dict) -> None:

        self._task_params = SimpleNamespace(**task_params)
        self._input_params = SimpleNamespace(**input_params)
        self._output_params = SimpleNamespace(**output_params)

        super().__init__(name=self._task_params.name, mode=self._task_params.mode, description=self._task_params.description)

        self._datasource_id = self._input_params.id
        self._destination = self._output_params.uri

    def execute(self) -> None:
        """Downloads Zip Files from the DataSource website."""

        task = self._setup()
        self._execute()
        self._teardown(task)

    def _execute(self) -> None:
        """Download the source data from each URL into the destination directory."""
        datasource = self._uow.datasource.get(self._datasource_id)
        for ds_url in datasource.urls.values():
            Downloader(url=ds_url.url, destination=self._destination).execute()
