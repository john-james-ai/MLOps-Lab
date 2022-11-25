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
# Modified   : Friday November 25th 2022 05:33:50 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Data Repository Module"""
import logging
import os
from abc import ABC, abstractmethod
from typing import Any
import shutil

from recsys.core.dal.dataset import Dataset
from recsys.core.dal.registry import DatasetRegistry
from recsys.core.services.io import IOService

# ------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #


class Repo(ABC):
    """Repository base class"""

    @abstractmethod
    def add(self, *args, **kwargs) -> None:
        pass

    @abstractmethod
    def get(self, id: str) -> Any:
        pass

    @abstractmethod
    def remove(self, id: str) -> None:
        pass

    @abstractmethod
    def exists(self, id: str) -> bool:
        pass


# ------------------------------------------------------------------------------------------------ #
class DatasetRepo(Repo):
    """Dataset Repository"""

    def __init__(
        self, directory: str, io: IOService, registry: DatasetRegistry, file_format: str
    ) -> None:
        self._directory = directory
        self._io = io
        self._registry = registry
        self._file_format = file_format

    def __len__(self) -> int:
        return len(self._registry)

    @property
    def directory(self) -> str:
        return self._directory

    def reset(self) -> None:
        msg = "Are you SURE. This will delete EVERYTHING in this repository. If you are sure, type 'YES, I AM SURE': "
        sure = input(msg)
        if sure == "YES, I AM SURE":
            logger.info(f"\n\nRESETTING REPOSITORY at {self._directory}.")
            self._registry.reset()
            shutil.rmtree(self._directory, ignore_errors=True)
            logger.info(f"\n\nREPOSITORY at {self._directory}, RESET.")

    def add(self, dataset: Dataset) -> None:
        """Adds a Dataset to the repo.

        Args:
            dataset (Dataset): The Dataset object
        """
        while self._registry.version_exists(dataset):
            dataset.version = dataset.version + 1

        dataset.filepath = self._get_filepath(dataset)
        dataset = self._registry.add(dataset)
        os.makedirs(os.path.dirname(dataset.filepath), exist_ok=True)
        self._io.write(filepath=dataset.filepath, data=dataset)
        return dataset

    def get(self, id: int) -> Any:
        """Retrieves a Dataset object by id

        Args:
            id (int): ID of the dataset object
        """
        try:
            registration = self._registry.get(id)
            return self._io.read(registration["filepath"])
        except FileNotFoundError:
            msg = f"No Dataset with id = {id} exists."
            logger.error(msg)
            raise FileNotFoundError(msg)

    def find_dataset(self, name: str, stage: str = None):
        """Returns a Dataframe of Dataset objects that match the criteria

        Args:
            name (str): Required name of Dataset.
            stage (str): Optional, one of 'raw', 'interim', or 'cooked'.
        """
        return self._registry.find_dataset(name=name, stage=stage)

    def remove(self, id: int, ignore_errors=False) -> None:
        """Removes a Dataset object from the registry and file system.

        Args:
            id (int): ID of the dataset object
            ignore_errors (bool): If True, errors will not throw an exception.

        Raises: FileNotFoundError (if ignore_errors = False)
        """
        try:
            registration = self._registry.get(id)
            os.remove(registration["filepath"])
            self._registry.remove(id)

        except FileNotFoundError:
            if ignore_errors:
                pass
            else:
                msg = f"Dataset id: {id} does not exist."
                logger.error(msg)
                raise FileNotFoundError(msg)

    def exists(self, id: int) -> bool:
        """Checks whether the dataset exists.
        Args:
            dataset (Dataset): The dataset to check.
        """
        return self._registry.exists(id)

    def version_exists(self, dataset: Dataset) -> bool:
        """Checks whether the dataset exists.
        Args:
            dataset (Dataset): The dataset to check.
        """
        return self._registry.version_exists(
            name=dataset.name, stage=dataset.stage, version=dataset.version
        )

    def print(self, id: int) -> None:
        """Prints a Dataset by id.

        Args:
            id (int): Id for a Dataset

        """
        try:
            registration = self._registry.get(id=id)
            dataset = Dataset(**registration)
            print(dataset)
        except FileNotFoundError:
            msg = f"Dataset id: {id} does not exist."
            logger.error(msg)
            raise FileNotFoundError(msg)

    def print_registry(self) -> None:
        """Prints list of dataset names in repository."""
        print(self._registry.get_all())

    def _get_filepath(self, dataset: Dataset) -> str:
        """Formats a filename using the Dataset name, stage, and version."""
        filename = (
            dataset.name
            + "_"
            + dataset.stage
            + "_v"
            + str(dataset.version)
            + "."
            + self._file_format
        )
        return os.path.join(self._directory, dataset.stage, filename)
