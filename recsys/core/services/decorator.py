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
# Modified   : Friday November 25th 2022 05:48:17 pm                                               #
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
from recsys.config.base import FilesetInput, DatasetInput

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

        datasets = {}
        dataset = None

        # Handle input
        if hasattr(self, "input_params"):
            if isinstance(self.input_params, FilesetInput):
                pass  # This will be handled by the operator.
            elif isinstance(self.input_params, DatasetInput):
                dataset = repo.get(self.input_params.id)
                setattr(self, "input_dataset", dataset.data)
            elif isinstance(self.input_params, dict):
                for k, v in self.input_params.items():
                    if isinstance(v, FilesetInput):
                        pass  # Again, handled by operator
                    elif isinstance(v, DatasetInput):
                        datasets[v.name] = repo.get(v.id)
                    else:
                        msg = f"{self.input_params} is unrecognized input."
                        logger.error(msg)
                        raise TypeError(msg)
                if len(datasets) > 0:
                    setattr(func, "input_dataset", datasets)

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
        repo.print_registry()

    return wrapper
