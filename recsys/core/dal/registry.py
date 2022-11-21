#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /registry.py                                                                        #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday November 15th 2022 03:18:48 pm                                              #
# Modified   : Sunday November 20th 2022 10:43:38 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Repository Registry Module"""
import logging
import os
import copy
import pandas as pd

from recsys.core.dal.dataset import Dataset
from recsys.core.base.registry import Registry
from recsys.core.services.io import IOService
from recsys.core.dal.dataset import get_id
from recsys.core.base.config import ENVS

# ------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #


class FileBasedRegistry(Registry):
    """File-based repository registry.

    Registry contains instances of DatasetMeta objects stored in a pickled dictionary.

    Args:
        directory (str): The base directory for the repository
        io (IOService): An instance of the type IOService
        file_format (str): The format in which Datasets are stored in the repo. This
            variable is used to construct filenames. Defaults to 'pkl'


    """

    __ids = {}
    __registry = {}
    __id_filename = "ids.pkl"
    __id_filepath = None
    __registry_filename = "registry.pkl"
    __registry_filepath = None

    def __init__(self, directory: str, io: IOService, file_format: str = "pkl") -> None:
        self._directory = directory
        self._io = io
        self._file_format = file_format
        self._filepath = os.path.join(self._directory, "registry.pkl")

        self._registry = {}
        self._load()

    def __len__(self) -> int:
        self._load()
        return len(self._registry)

    @property
    def count(self) -> int:
        try:
            return len(self._registry)
        except TypeError:
            return 0

    def add(self, dataset: Dataset) -> None:
        """Registers a Dataset object.

        Args:
            dataset (Dataset): The Dataset object to be registered.
        """
        self._load()
        dataset = self._set_id(dataset=dataset)
        dataset = self._add_filepath(dataset)
        if self.exists(dataset.filepath):
            if dataset.version_control is True:
                dataset = self._bump_version_update_dataset(dataset)
                self.add(dataset=dataset)
            else:
                msg = "Dataset id {} is already registered.".format(dataset.id)
                logger.error(msg)
                raise FileExistsError(msg)
        else:
            self._registry[dataset.id] = dataset.as_meta()
            self._commit_changes()
        return dataset

    def get(self, id: str) -> dict:
        """Obtains metadata for Dataset object.

        Args:
            id (str): ID of the Dataset object.

        Returns DatasetRegistration object.
        """
        self._load()
        try:
            return self._registry[id]
        except KeyError:
            msg = "No Dataset object with id {} is registered. Registry may be corrupt.".format(id)
            logger.error(msg)
            raise FileNotFoundError(msg)

    def remove(self, id: str) -> None:
        """Removes Dataset from registry

        Args:
            id (str): ID of the Dataset object.
        """
        self._load()
        try:
            _ = self._registry[id]
            del self._registry[id]
            self._save()
        except KeyError:
            logger.error("Dataset id {} is not registered.".format(id))

    def exists(self, id: str) -> bool:
        self._load()
        return id in self._registry.keys()

    def to_dataframe(self) -> pd.DataFrame:
        """Returns a metadata list of all registered Dataset objects."""
        self._load()
        datasets = []
        for name, dataset in self._registry.items():
            datasets.append(dataset)
        # Pandas DataFrame format provides a reasonable structure for reporting DatasetMeta objects.
        try:
            df = pd.DataFrame(data=datasets)
        except ValueError:
            df = pd.DataFrame(data=datasets, index=[0])
        return df

    def print_datasets(self) -> None:
        """Prints a DataFrame of items in the DataRegistry."""
        self._load()
        print(self.list_datasets)

    def _set_id(self, dataset: Dataset) -> int:
        """Assigns a unique id to the Dataset object."""
        dataset.id = copy.copy(self.__ids[dataset.env])
        self.__ids[dataset.env] = self.__ids[dataset.env] + 1
        return dataset

    def _bump_version_update_dataset(self, dataset: Dataset) -> Dataset:
        # Store old Dataset for reporting
        old_dataset = copy.deepcopy(dataset)
        # Bump Version
        dataset.version = dataset.version + 1
        # Update ID
        dataset.id = get_id(
            name=dataset.name, env=dataset.env, version=dataset.version, stage=dataset.stage
        )
        # Change Filepath
        dataset = self._add_filepath(dataset=dataset)
        # Report
        logger.info(
            f"Dataset {old_dataset.id} with version: {old_dataset.version} is now Dataset {dataset.id} with version: {dataset.version}."
        )

        return dataset

    def _load(self, force: bool = False) -> None:
        """Loads the registry if not already loaded. Setting force to True will overwrite memory version."""

        self._load_registry()
        self._load_ids()

    def _load_registry(self) -> None:
        try:
            self._registry = self._io.read(self._filepath)
        except FileNotFoundError:
            self._registry = {}

    def _load_ids(self) -> None:
        try:
            self.__ids = self._io.read(self.__id_filepath)
        except FileNotFoundError:
            self._initialize_ids()

    def _initialize_ids(self) -> None:
        """If the registry exists, ids are set as 1+max(id) in an environment. Otherwise, set to 1."""
        self.__ids = {env: 1 for env in ENVS}
        if len(self._registry) > 0:
            max_ids = self._registry[["env", "id"]].groupby(["env"], as_index=False).max()
            ids = {"env": max_ids["env"].values, "id": max_ids["id"].values + 1}
            for env, id in ids.items():
                self.__ids[env] = id
        logger.debug("Ids initialized as follows: \n {}".format(self.__ids))

    def _save(self) -> None:
        """Saves the registry dictionary to file."""
        os.makedirs(os.path.dirname(self._filepath), exist_ok=True)
        self._io.write(filepath=self._filepath, data=self._registry)

    def _add_filepath(self, dataset: Dataset) -> Dataset:
        dataset.filename = dataset.name + "_v" + str(dataset.version) + "." + self._file_format
        dataset.filepath = os.path.join(
            self._directory, dataset.env, dataset.stage, dataset.filename
        )
        return dataset
