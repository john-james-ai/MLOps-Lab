#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /repo.py                                                                            #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday November 14th 2022 01:27:04 am                                               #
# Modified   : Sunday November 20th 2022 09:17:20 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Data Repository Module"""
import logging
import os
from abc import abstractmethod
from typing import Any
from functools import wraps

from dependency_injector import containers, providers
from dependency_injector.wiring import inject

from recsys.core.base.repo import Repo
from recsys.core.dal.registry import FileBasedRegistry, Registry
from recsys.core.dal.dataset import Dataset
from recsys.core.services.io import IOService
from recsys.core.base.config import FILE_FORMATS, DATA_REPO_FILE_FORMAT, TEST_REPO_DIR

# ------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #


class DatasetRepo(Repo):
    """Dataset Repository"""

    def __init__(
        self,
        registry: Registry,
        io: IOService,
        directory: str,
        file_format: str = DATA_REPO_FILE_FORMAT,
        version_control: bool = True,
    ) -> None:
        self._registry = registry
        self._io = io
        self._directory = directory
        self._file_format = file_format
        self._validate()

    def __len__(self) -> int:
        return len(self._registry)

    @property
    def count(self) -> int:
        return self._registry.count

    def add(self, dataset: Dataset) -> None:
        """Adds a Dataset to the repo and saves the data to file.

        Args:
            dataset (Dataset): The Dataset object
        """
        dataset = self._registry.add(dataset)
        self._io.write(filepath=dataset.filepath, data=dataset)
        return dataset

    def get(self, id: str) -> Any:
        """Retrieves a Dataset object by id

        Args:
            id (str): ID of the dataset object
        """
        meta = self._registry.get(id=id)
        data = self._io.read(filepath=meta.filepath)
        dataset = Dataset(
            name=meta.name,
            env=meta.env,
            stage=meta.stage,
            description=meta.description,
            filepath=meta.filepath,
            cost=meta.cost,
            id=meta.id,
            data=data,
        )
        dataset.created = meta.created
        dataset.creator = meta.creator
        return dataset

    def remove(self, id: str) -> None:
        """Removes a Dataset object from the registry and file system.

        Args:
            id (str): ID of the dataset object

        Raises: FileNotFoundError
        """
        meta = self._registry.get(id)
        try:
            os.remove(meta.filepath)
        except FileNotFoundError as e:
            logger.error("Dataset {} not found at {}".format(id, meta.filepath))
            raise (e)
        self._registry.remove(id)

    def exists(self, id: str) -> bool:
        """Checks whether the dataset is registered and data (if any) exists on file.
        Args:
            id (str): ID of the dataset object
        """
        try:
            meta = self._registry.get(id=id)
            return os.path.exists(meta.filepath)
        except FileNotFoundError:
            return False

    def list_datasets(self) -> list:
        """Return a list of Dataset names."""
        return self._registry.list_datasets()

    def print_datasets(self) -> None:
        """Prints list of dataset names in repository."""
        print(self.list_datasets())

    def _validate(self) -> None:
        if self._file_format not in FILE_FORMATS:
            msg = f"File format {self._file_format} is not supported. Valid file formats are {FILE_FORMATS}."
            logger.error(msg)
            raise ValueError(msg)


# ------------------------------------------------------------------------------------------------ #
#                                       CONTAINER                                                  #
# ------------------------------------------------------------------------------------------------ #
class Container(containers.DeclarativeContainer):

    # Services
    registry = providers.Singleton(FileBasedRegistry, directory=TEST_REPO_DIR, io=IOService)

    repo = providers.Singleton(
        DatasetRepo, registry=registry, directory=TEST_REPO_DIR, io=IOService
    )


# ------------------------------------------------------------------------------------------------ #
#                                   DECORATOR FUNCTIONS                                            #
# ------------------------------------------------------------------------------------------------ #
def repository(func):
    @wraps(func)
    @inject
    def wrapper(self, *args, **kwargs):

        container = Container()
        repo = container.repo()

        datasets = None

        if hasattr(self, "dataset_in_id"):
            if isinstance(self.dataset_in_id, list):
                datasets = []
                for id in self.dataset_in_id:
                    datasets.append(repo.get(id))
            else:
                datasets = repo.get(self.dataset_in_id)

            if datasets is not None:
                setattr(func, "input_data", datasets)

        result = func(self, *args, **kwargs)

        if isinstance(result, Dataset):
            result = repo.add(result)
            return result
        else:
            msg = "Result was not a Dataset object."
            logger.error("Result was not a Dataset object.")
            raise TypeError(msg)

    return wrapper
