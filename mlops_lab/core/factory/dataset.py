#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Enter Project Name in Workspace Settings                                            #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /mlops_lab/core/factory/dataset.py                                                  #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : Enter URL in Workspace Settings                                                     #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday January 14th 2023 03:57:28 pm                                              #
# Modified   : Tuesday January 24th 2023 08:13:41 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2023 John James                                                                 #
# ================================================================================================ #
"""Dataset Factory"""
from mlops_lab.core.factory.base import Factory
from mlops_lab.core.entity.dataset import Dataset, DataFrame


# ------------------------------------------------------------------------------------------------ #
#                                     DATASET FACTORY                                              #
# ------------------------------------------------------------------------------------------------ #
class DatasetFactory(Factory):
    def __init__(self) -> None:
        super().__init__()
        self._instance = None

    def __call__(self, config: dict) -> Dataset:
        if not self._instance:
            self._instance = self._build_entity(config)
        return self._instance

    def _build_entity(self, config: dict) -> Dataset:
        dataset = Dataset(**config)
        return dataset


# ------------------------------------------------------------------------------------------------ #
#                                     DATAFRAME FACTORY                                            #
# ------------------------------------------------------------------------------------------------ #
class DataFrameFactory(Factory):
    def __init__(self) -> None:
        super().__init__()
        self._instance = None

    def __call__(self, config: dict) -> DataFrame:
        if not self._instance:
            self._instance = self._build_entity(config)
        return self._instance

    def _build_entity(self, config: dict) -> DataFrame:
        dataframe = DataFrame(**config)
        return dataframe
