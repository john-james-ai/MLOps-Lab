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
# Modified   : Saturday November 26th 2022 08:16:54 pm                                             #
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
        if hasattr(self, "input_params"):
            if not self.input_params.get("filepath", None):
                dataset = repo.get_dataset(self.input_params["name"], self.input_params["stage"])
                setattr(self, "input_dataset", dataset.data)

        # Execute wrapped method.
        result = func(self, *args, **kwargs)

        # Handle Results

        results = {}

        def store_result(result) -> None:
            if isinstance(result, dict):
                for k, v in result.items():
                    results[k] = repo.add(v)
                return results
            elif isinstance(result, Dataset):
                return repo.add(result)
            else:
                msg = "Result was not a Dataset or dictionary object."
                logger.error(msg)
                raise TypeError(msg)

        return store_result(result)

    return wrapper
