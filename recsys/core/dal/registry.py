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
# Modified   : Saturday November 19th 2022 01:30:54 am                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Repository Registry Module"""
import logging
import os

import pandas as pd

from recsys.core.dal.dataset import Dataset
from recsys.core.base.registry import Registry
from recsys.core.services.io import IOService
from recsys.core.dal.dataset import get_id

# ------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #


class FileBasedRegistry(Registry):
    """File-based repository registry.

    Args:
        env (str): One of 'dev', 'test', 'prod'.

    """

    __format = "pkl"

    def __init__(self, directory: str, io: IOService) -> None:
        self._directory = directory
        self._io = io
        self._filepath = os.path.join(self._directory, "registry.pkl")
        self._registry = None

    def __len__(self) -> int:
        self._load()
        return len(self._registry)

    @property
    def io(self) -> None:
        return self._io

    @property
    def directory(self) -> None:
        return self._directory

    def add(self, dataset: Dataset) -> None:
        """Registers a Dataset object.

        Args:
            dataset (Dataset): The Dataset object to be registered.
        """
        meta = dataset.as_meta()
        self._load()  # Load the registry dictionary if not already loaded
        if self.exists(meta.id):
            if dataset.versioning:
                dataset.version += 1
                dataset.id = get_id(
                    name=dataset.name, env=dataset.env, version=dataset.version, stage=dataset.stage
                )
                self.add(dataset=dataset)
            else:
                msg = "Dataset named {} is already registered.".format(meta.name)
                logger.error(msg)
                raise FileExistsError(msg)
        else:
            self._registry[meta.id] = meta
            self._save()

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

    def list_datasets(self) -> list:
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

    def _load(self, force: bool = False) -> None:
        """Loads the registry if not already loaded. Setting force to True will overwrite memory version."""
        if self._registry is None or force:
            try:
                self._registry = self._io.read(self._filepath)
            except FileNotFoundError:
                self._registry = {}

    def _save(self) -> None:
        """Saves the registry dictionary to file."""
        os.makedirs(os.path.dirname(self._filepath), exist_ok=True)
        self._io.write(filepath=self._filepath, data=self._registry)
