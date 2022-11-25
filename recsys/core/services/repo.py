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
# Modified   : Thursday November 24th 2022 04:42:50 am                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Data Repository Module"""
import logging
import os
from dotenv import load_dotenv
from abc import ABC, abstractmethod
from typing import Any
from functools import wraps
import shutil


from dependency_injector import containers, providers
from dependency_injector.wiring import inject

from recsys.core.dal.dataset import Dataset
from recsys.core.dal.database import Database
from recsys.core.dal.registry import DatasetRegistry
from recsys.core.services.io import IOService
from recsys.config.base import REPO_DIRS, FilesetInput, DatasetInput

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

    __registry = {}

    def __init__(self, io: IOService, registry: DatasetRegistry, file_format: str = "pkl") -> None:
        self._io = io
        DatasetRepo.__registry = registry
        self._file_format = file_format

        self._directory = self._get_directory()

        self._validate()

    def __len__(self) -> int:
        return len(DatasetRepo.__registry)

    @property
    def directory(self) -> str:
        return self._directory

    def reset(self) -> None:
        DatasetRepo.__registry.reset()
        shutil.rmtree(self._directory, ignore_errors=True)

    def add(self, dataset: Dataset) -> None:
        """Adds a Dataset to the repo.

        Args:
            dataset (Dataset): The Dataset object
        """
        while DatasetRepo.__registry.version_exists(dataset):
            dataset.version = dataset.version + 1

        dataset.filepath = self._get_filepath(dataset)
        dataset = DatasetRepo.__registry.add(dataset)
        os.makedirs(os.path.dirname(dataset.filepath), exist_ok=True)
        self._io.write(filepath=dataset.filepath, data=dataset)
        return dataset

    def get(self, id: int) -> Any:
        """Retrieves a Dataset object by id

        Args:
            id (int): ID of the dataset object
        """
        try:
            registration = DatasetRepo.__registry.get(id)
            return self._io.read(registration["filepath"])
        except FileNotFoundError:
            msg = f"No Dataset with id = {id} exists."
            logger.error(msg)
            raise FileNotFoundError(msg)

    def find_dataset(self, name: str, env: str = None, stage: str = None):
        """Returns a Dataframe of Dataset objects that match the criteria

        Args:
            name (str): Required name of Dataset.
            env (str): Optional, unless stage is provided. One of 'test', 'dev', or 'prod'.
            stage (str): Optional, one of 'raw', 'interim', or 'cooked'.
        """
        return DatasetRepo.__registry.find_dataset(name=name, env=env, stage=stage)

    def remove(self, id: int, ignore_errors=False) -> None:
        """Removes a Dataset object from the registry and file system.

        Args:
            id (int): ID of the dataset object
            ignore_errors (bool): If True, errors will not throw an exception.

        Raises: FileNotFoundError (if ignore_errors = False)
        """
        try:
            registration = DatasetRepo.__registry.get(id)
            os.remove(registration["filepath"])
            DatasetRepo.__registry.remove(id)

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
        return DatasetRepo.__registry.exists(id)

    def version_exists(self, dataset: Dataset) -> bool:
        """Checks whether the dataset exists.
        Args:
            dataset (Dataset): The dataset to check.
        """
        return DatasetRepo.__registry.version_exists(
            name=dataset.name, env=dataset.env, stage=dataset.stage, version=dataset.version
        )

    def print(self, id: int) -> None:
        """Prints a Dataset by id.

        Args:
            id (int): Id for a Dataset

        """
        try:
            registration = DatasetRepo.__registry.get(id=id)
            dataset = Dataset(**registration)
            print(dataset)
        except FileNotFoundError:
            msg = f"Dataset id: {id} does not exist."
            logger.error(msg)
            raise FileNotFoundError(msg)

    def print_registry(self) -> None:
        """Prints list of dataset names in repository."""
        print(DatasetRepo.__registry.get_all())

    def _validate(self) -> None:
        if not isinstance(self._io, (type(IOService), IOService)):
            msg = f"{type(self._io)} is not a valid io service."
            logger.error(msg)
            raise ValueError(msg)
        if not isinstance(DatasetRepo.__registry, DatasetRegistry):
            msg = f"{type(DatasetRepo.__registry)} is not a valid registry object."
            logger.error(msg)
            raise ValueError(msg)

    def _get_directory(self) -> str:
        """Returns the repo directory based upon the current environment."""
        load_dotenv()
        ENV = os.getenv("ENV")
        try:
            return REPO_DIRS["data"].get(ENV)
        except KeyError:
            msg = "The current environment, specified by the 'ENV' variable in the .env file, is not supported."
            logger.error(msg)
            raise ValueError(msg)

    def _get_filepath(self, dataset: Dataset) -> str:
        """Formats a filename using the Dataset name, environment, stage, and version."""
        filename = (
            dataset.name
            + "_"
            + dataset.env
            + "_"
            + dataset.stage
            + "_v"
            + str(dataset.version)
            + "."
            + self._file_format
        )
        return os.path.join(self._directory, dataset.env, dataset.stage, filename)


# ------------------------------------------------------------------------------------------------ #
#                                       CONTAINER                                                  #
# ------------------------------------------------------------------------------------------------ #


class Container(containers.DeclarativeContainer):

    # Services
    db = providers.Singleton(Database, database="data")

    registry = providers.Singleton(DatasetRegistry, database=db)

    repo = providers.Singleton(DatasetRepo, io=IOService, registry=registry, file_format="pkl")


# ------------------------------------------------------------------------------------------------ #
#                                   DECORATOR FUNCTIONS                                            #
# ------------------------------------------------------------------------------------------------ #
def repository(func):
    @wraps(func)
    @inject
    def wrapper(self, *args, **kwargs):

        container = Container()
        repo = container.repo()

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
