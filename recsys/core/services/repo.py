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
# Modified   : Tuesday November 22nd 2022 01:56:58 am                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Data Repository Module"""
import logging
import os
from dotenv import load_dotenv
import pandas as pd
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any
from functools import wraps
from pprint import pprint
import shutil

from dependency_injector import containers, providers
from dependency_injector.wiring import inject

from recsys.core.dal.dataset import Dataset
from recsys.core.services.io import IOService
from recsys.core import REPO_FILE_FORMAT, REPO_DIRS

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

    def __init__(
        self,
        io: IOService,
        file_format: str = REPO_FILE_FORMAT,
        version_control: bool = True,
    ) -> None:
        self._io = io
        self._file_format = file_format

        self._directory = self._get_directory()
        self._version_control = version_control

        self._validate()

    def __len__(self) -> int:
        return len(DatasetRepo.__registry)

    def reset(self) -> None:
        DatasetRepo.__registry = {}
        shutil.rmtree(self._directory, ignore_errors=True)

    def add(self, dataset: Dataset) -> None:
        """Adds a Dataset to the repo.

        Args:
            dataset (Dataset): The Dataset object
        """

        if self.exists(dataset):
            if self._version_control:
                dataset.version += 1
                msg = f"The version of Dataset {dataset.id}:{dataset.name} has been bumped to {dataset.version}"
                logger.info(msg)
                self.add(dataset)
            else:
                msg = "A Dataset with same name, environment, stage, and version exists. For version control, reinstantiate the Repo with version_control = True"
                logger.error(msg)
                raise FileNotFoundError(msg)
        else:
            filepath = self._get_filepath(dataset)
            registration = dataset.as_dict()
            registration["filepath"] = filepath
            DatasetRepo.__registry[str(dataset.id)] = registration
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            self._io.write(filepath=filepath, data=dataset)
        return dataset

    def get(self, id: int) -> Any:
        """Retrieves a Dataset object by id

        Args:
            id (int): ID of the dataset object
        """
        try:
            registration = DatasetRepo.__registry[str(id)]
            return Dataset(**registration)
        except KeyError:
            msg = f"No Dataset with id = {id} exists."
            logger.error(msg)
            raise FileNotFoundError(msg)

    def remove(self, id: int, ignore_errors=False) -> None:
        """Removes a Dataset object from the registry and file system.

        Args:
            id (int): ID of the dataset object
            ignore_errors (bool): If True, errors will not throw an exception.

        Raises: FileNotFoundError (if ignore_errors = False)
        """
        try:
            del DatasetRepo.__registry[str(id)]
        except KeyError:
            if ignore_errors:
                pass
            else:
                msg = f"Dataset id: {id} does not exist."
                logger.error(msg)
                raise FileNotFoundError(msg)

    def exists(self, dataset: Dataset) -> bool:
        """Checks whether the dataset exists.
        Args:
            dataset (Dataset): The dataset to check.
        """
        registrations = self._registry_as_df()

        if registrations is None:
            return False
        else:
            logger.debug("\n\nRegistrations")
            logger.debug(f"\n{registrations}")
            logger.debug(f"\n{registrations.columns}")
            item = registrations.loc[
                (registrations["name"] == dataset.name)
                & (registrations["env"] == dataset.env)
                & (registrations["stage"] == dataset.stage)
                & (registrations["version"] == dataset.version)
            ]
        return len(item) > 0

    def print(self, id: int) -> None:
        """Prints a Dataset by id.

        Args:
            id (int): Id for a Dataset

        """
        try:
            registration = self.get(str(id))
            dataset = Dataset(**registration)
            dataset.data = self._io.read(registration["filepath"])
            d = dataset.as_dict()
            pprint(d)
        except FileNotFoundError:
            msg = f"Dataset id: {id} does not exist."
            logger.error(msg)
            raise FileNotFoundError(msg)

    def print_registry(self) -> None:
        """Prints list of dataset names in repository."""
        print(self._registry_as_df())

    def _validate(self) -> None:
        if self._file_format not in self._io().file_formats:
            msg = f"File format {self._file_format} is not supported. Valid file formats are {self._io.file_formats}."
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

    def _registry_as_df(self) -> pd.DataFrame:
        """Return a list of Dataset names."""
        lod = []
        for k, v in DatasetRepo.__registry.items():
            lod.append(v)
        if len(lod) == 0:
            return None
        elif len(lod) == 1:
            return pd.DataFrame(data=lod, index=[0])
        else:
            return pd.DataFrame(data=lod)


# ------------------------------------------------------------------------------------------------ #
#                                       CONTAINER                                                  #
# ------------------------------------------------------------------------------------------------ #
class Container(containers.DeclarativeContainer):

    # Services
    repo = providers.Singleton(DatasetRepo, io=IOService, file_format="pkl")


# ------------------------------------------------------------------------------------------------ #
#                                   DECORATOR FUNCTIONS                                            #
# ------------------------------------------------------------------------------------------------ #
def repository(func):
    @wraps(func)
    @inject
    def wrapper(self, *args, **kwargs):

        started = datetime.now()

        container = Container()
        repo = container.repo()

        datasets = None

        if hasattr(self, "dataset_in"):
            if isinstance(self.dataset_in, list):
                datasets = {}
                for id in self.dataset_in:
                    try:
                        datasets[str(id)] = repo.get(id)
                    except FileNotFoundError:
                        msg = f"Input Dataset id: {id} does not exist."
                        logger.error(msg)
                        raise FileNotFoundError(msg)

            else:
                try:
                    datasets = repo.get(self.dataset_in)
                except FileNotFoundError:
                    msg = f"Input Dataset id: {self.dataset_in} does not exist."
                    logger.error(msg)
                    raise FileNotFoundError(msg)

            if datasets is not None:
                setattr(func, "input_data", datasets)

        result = func(self, *args, **kwargs)

        ended = datetime.now()
        duration = (started - ended).total_seconds()

        def store_result(result, duration) -> None:
            if isinstance(result, dict):
                for k, v in result.items():
                    store_result(v, duration)
            elif isinstance(result, Dataset):
                result.cost = duration
                return repo.add(result)
            else:
                msg = "Result was not a Dataset object."
                logger.error(msg)
                raise TypeError(msg)

        return store_result(result, duration)

    return wrapper
