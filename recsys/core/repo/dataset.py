#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/repo/dataset.py                                                        #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday December 8th 2022 04:07:04 pm                                              #
# Modified   : Wednesday December 14th 2022 03:18:51 am                                            #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import os
import pandas as pd

from .base import Repo
from recsys.core.dal.dao import DAO
from recsys.core.services.io import IOService
from recsys.core.entity.dataset import Dataset
# ------------------------------------------------------------------------------------------------ #


class DatasetRepo(Repo):
    """Repository base class"""

    def __init__(self, dataset: Dataset, dao: DAO, io: IOService) -> None:
        super().__init__()
        self._dataset = dataset
        self._dao = dao
        self._io = io

    def __len__(self) -> int:
        return len(self._dao)

    def add(self, dataset: Dataset) -> Dataset:
        """Adds a Dataset to the repository and returns the Dataset with the id added."""
        # Set the URI based upon datasource, workspace, stage, and filename.
        dataset = self._set_uri(dataset)
        # Persist the data
        if dataset.data is not None:
            self._io.write(filepath=dataset.uri, data=dataset.data)
        # Convert Dataset to DTO and submit the Dataset dto to the database.
        dto = dataset.as_dto()
        dto = self._dao.create(dto)
        # Convert the dto with id, back to a Dataset and return.
        return self._dataset.from_dto(dto)

    def get(self, id: int) -> Dataset:
        "Returns a Dataset with the designated id"
        dto = self._dao.read(id)
        dataset = self._dataset.from_dto(dto)
        return self._get_data_if_exists(dataset)

    def get_by_name(self, name: str) -> Dataset:
        """Returns a Dataset with the specified name."""
        dto = self._dao.read_by_name(name)
        dataset = self._dataset.from_dto(dto)
        return self._get_data_if_exists(dataset)

    def update(self, dataset: Dataset) -> None:
        """Updates a Dataset in the databases."""
        # Set the URI based upon datasource, workspace, stage, and filename.
        dataset = self._set_uri(dataset)
        # Persist the data
        if dataset.data is not None:
            self._io.write(filepath=dataset.uri, data=dataset.data)
        dto = dataset.as_dto()
        self._dao.update(dto)

    def remove(self, id: int) -> None:
        """Removes a Dataset with id from repository and deletes the enclosed data from disk."""
        dataset = self._dao.read(id)
        self._dao.delete(id)
        if dataset.uri is not None:
            if os.path.exists(dataset.uri):
                delete = input(f"This will permanently delete {dataset.uri}. Take a minute and type 'y' if you are sure! (y/n)")
                if 'y' in delete:
                    os.remove(dataset.uri)
                    msg = f"File at {dataset.uri} has been deleted."
                    self._logger.info(msg)

    def exists(self, id: int) -> bool:
        """Returns True if entity with id exists in the repository."""
        return self._dao.exists(id)

    def save(self) -> None:
        self._dao.save()

    def print(self, id: int = None) -> None:
        """Prints the repository contents as a DataFrame."""
        if id is not None:
            self._print_by_id(id)
        else:
            self._print_all()

    def _print_by_id(self, id: int) -> None:
        dataset = self._dao.read(id)
        print(dataset)

    def _print_all(self) -> None:
        datasets = self._dao.read_all()
        df = pd.DataFrame.from_dict(data=datasets, orient='index')
        print(df)

    def _set_uri(self, dataset) -> Dataset:
        """Sets the URI based upon the datasource, workspace, stage, and filename."""
        filename = os.path.basename(dataset.filename) + ".pkl"
        dataset.uri = os.path.join("data", dataset.datasource, "workspaces", dataset.workspace, dataset.stage, filename)
        return dataset

    def _get_data_if_exists(self, dataset: Dataset) -> Dataset:
        """Obtains data if data are present at a non-null uri."""
        if dataset.uri is not None:
            if os.path.exists(dataset.uri):
                dataset.data = IOService.read(filepath=dataset.uri)
        return dataset
