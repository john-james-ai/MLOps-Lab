#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /decorator.py                                                                       #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday November 14th 2022 01:27:04 am                                               #
# Modified   : Sunday November 27th 2022 04:53:26 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Repository decorator for DatasetOperator classes."""
import logging
from functools import wraps

from dependency_injector.wiring import Provide, inject

from recsys.core.services.container import Container
from recsys.core.dal.dataset import Dataset
from recsys.core.dal.repo import DatasetRepo

# ------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #

# ------------------------------------------------------------------------------------------------ #
#                                   DECORATOR FUNCTIONS                                            #
# ------------------------------------------------------------------------------------------------ #


@inject
def repository(func, repo: DatasetRepo = Provide[Container.repo]):  # noqa C109
    @wraps(func)
    def wrapper(self, *args, **kwargs):

        dataset = None

        # Handle input
        if not hasattr(self.input_params, "filepath"):
            dataset = repo.get_dataset(name=self.input_params.name, stage=self.input_params.stage)
            setattr(self, "input_data", dataset.data)

        # Execute wrapped method.
        result = func(self, *args, **kwargs)

        # Handle Results

        results = {}

        def store_result(result) -> None:
            if isinstance(result, Dataset):
                return repo.add(result)
            elif isinstance(result, dict):
                for name, dataset in result.items():
                    results[name] = store_result(dataset)
                return results
            else:
                msg = f"Result type: {type(result)} is not supported."
                logger.error(msg)
                raise TypeError(msg)

        return store_result(result)

    return wrapper
