#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/factory/dataset.py                                                     #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday January 14th 2023 03:57:28 pm                                              #
# Modified   : Saturday January 14th 2023 09:47:49 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2023 John James                                                                 #
# ================================================================================================ #
"""Dataset Factory"""
from recsys.core.factory.base import Factory
from recsys.core.entity.dataset import Dataset, DataFrame


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
