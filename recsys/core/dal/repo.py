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
# Modified   : Friday November 18th 2022 09:56:08 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Data Repository Module"""
import logging
import os
from abc import abstractmethod
from typing import Any, List, Union
from functools import wraps
from atelier.utils.datetimes import Timer

from recsys.core.base.registry import Registry
from recsys.core.dal.config import DatasetRepoConfigFR, DatasetRepoConfig
from recsys.core.base.repo import Repo, RepoBuilder, RepoDirector
from recsys.core.dal.registry import FileBasedRegistry
from recsys.core.dal.dataset import Dataset
from recsys.core.services.io import IOService
from recsys.core.base.config import FILE_FORMATS, DATA_REPO_FILE_FORMAT

# ------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #


class DatasetRepo(Repo):
    """Dataset Repository"""

    __shared_state = dict()

    def __init__(self) -> None:
        self.__dict__ = self.__shared_state
        self._directory = None
        self._file_format = DATA_REPO_FILE_FORMAT

        # Variables set by builder via setters
        self._registry = None
        self._io = None

    def __len__(self) -> int:
        return len(self._registry)

    @property
    def directory(self) -> str:
        return self._directory

    @directory.setter
    def directory(self, directory: str) -> str:
        self._directory = directory

    @property
    def file_format(self) -> str:
        return self._file_format

    @file_format.setter
    def file_format(self, file_format: str) -> str:
        self._validate()
        self._file_format = file_format

    @property
    def io(self) -> IOService:
        return self._io

    @io.setter
    def io(self, io: IOService) -> None:
        self._io = io

    @property
    def registry(self) -> Registry:
        return self._registry

    @registry.setter
    def registry(self, registry: Registry) -> None:
        self._registry = registry

    def add(self, dataset: Dataset, versioning: bool = False) -> None:
        """Adds a Dataset to the repo and saves the data to file.

        If versioning is True, the version is bumped if the file exists and returned.
        Otherwise, attempting to add a duplicate file (one with the same, name, env, stage,
        and version) will raise an exception.

        Args:
            dataset (Dataset): The Dataset object
        """
        dataset = self._register_dataset(dataset, versioning)
        self._io.write(filepath=dataset.filepath, data=dataset)
        return dataset

    def get(self, id: str) -> Any:
        """Retrieves a Dataset object by id

        Args:
            id (str): ID of the dataset object
        """
        meta = self._registry.get(id=id)
        return self._io.read(filepath=meta.filepath)

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

    def _register_dataset(self, dataset: Dataset, versioning: bool = False) -> Dataset:
        """Register's the dataset and handles exceptions.

        Args:
            dataset (Dataset): The Dataset object
        """
        filename = dataset.name + "." + self._file_format
        dataset.filepath = os.path.join(
            self._directory, dataset.env, dataset.version, dataset.stage, filename
        )
        self._registry.add(dataset, versioning)
        return dataset


# ------------------------------------------------------------------------------------------------ #


class DatasetRepoBuilder(RepoBuilder):
    """Base class for Dataset repository builders"""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__()

    @abstractmethod
    def build_config(self, config: DatasetRepoConfig()) -> None:
        pass

    @abstractmethod
    def build_registry(self) -> None:
        pass

    @abstractmethod
    def build_repo(self) -> None:
        pass


# ------------------------------------------------------------------------------------------------ #


class DatasetRepoBuilderFR(DatasetRepoBuilder):
    """Constructs a Dataset Repository."""

    def __init__(self) -> None:
        self._config = None

    @property
    def repo(self) -> Repo:
        return self._repo

    def build_config(self, config: DatasetRepoConfigFR) -> None:
        self._config = config

    def build_registry(self) -> None:

        self._registry = FileBasedRegistry(directory=self._config.directory, io=self._config.io)

    def build_repo(self) -> None:
        """Constructs the Dataset Repo."""
        self._repo = DatasetRepo()
        self._repo.directory = self._config.directory
        self._repo.file_format = self._config.file_format
        self._repo.io = self._config.io
        self._repo.registry = self._registry


# ------------------------------------------------------------------------------------------------ #


class DatasetRepoDirector(RepoDirector):
    """Director responsible for operating the DataseRepotBuilder subclass methods in a sequence.

    Args:
        config (DatasetRepoConfig): The configuration for dataset repositories
        builder (DatasetRepoBuilder): The concrete dataset builder object

    """

    def __init__(self, config: DatasetRepoConfig, builder: DatasetRepoBuilder) -> None:
        super().__init__(config=config, builder=builder)

    def build_dataset_repo_with_file_registry(self) -> None:
        self._builder.build_config(self._config)
        self._builder.build_registry()
        self._builder.build_repo()


# ------------------------------------------------------------------------------------------------ #
def repository(func):
    @wraps(func)
    def wrapper(*args, **kwargs):

        input_dataset_params = kwargs.get("input_dataset_params", None)

        if input_dataset_params is not None:

            repo = DatasetRepo()

            if isinstance(input_dataset_params, list):
                datasets = []
                for dataset_params in input_dataset_params:
                    datasets.append(repo.get(id=dataset_params.id))
            else:
                datasets = repo.get(id=input_dataset_params.id)

            setattr(
                func, "input_data", datasets
            )  # Apparently dot notation assignment accomplishes the same thing.

        execute_intercept_and_update(func, repo)

    return wrapper

    def execute_intercept_and_update(repo, args, kwargs) -> Union[List[Dataset], Dataset]:
        """Wraps the decorated method in a timer, then updates the output with the cost (duration).

        Args:
            func (Callable): The decorated method
            repo (DatasetRepo): The Dataset repository

        Returns a Dataset, or a list of Datasets, as per the output of the wrapped method.

        """

        timer = Timer()
        results = func(*args, **kwargs)
        timer.stop()

        if isinstance(results, list):
            for result in results:
                setattr(result, "cost", timer.duration)
                repo.add(result)

        elif isinstance(results, dict):
            for _, result in results.items():
                setattr(result, "cost", timer.duration)
                repo.add(result)

        else:
            setattr(results, "cost", timer.duration)
            repo.add(results)
