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
# Modified   : Tuesday December 13th 2022 08:31:45 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import pandas as pd
from dependency_injector.wiring import Provide, inject

from .base import Repo
from recsys.core.dal.dao import DAO
from recsys.core.entity.dataset import Dataset
from recsys.core.entity.fileset import Fileset
from recsys.containers import Recsys
# ------------------------------------------------------------------------------------------------ #


class DatasetRepo(Repo):
    """Repository base class"""

    @inject
    def __init__(self,
                 dataset: Dataset = Dataset, fileset: Fileset = Fileset,
                 dataset_dao: DAO = Provide[Recsys.dao.dataset],
                 fileset_dao: DAO = Provide[Recsys.dao.fileset]) -> None:
        self._dataset = dataset
        self._dataset_dao = dataset_dao
        self._fileset = fileset
        self._fileset_dao = fileset_dao

    def __len__(self) -> int:
        return len(self._dataset_dao)

    def add(self, dataset: Dataset) -> Dataset:
        """Adds a Dataset to the repository and returns the Dataset with the id added."""
        fileset = dataset.fileset        
        fileset_dto = fileset.as_dto()
        dataset_dto = dataset.as_dto()
        # Exchange IDs
        fileset_dto.dataset_id = dataset.id
        dataset_dto.fileset_id = fileset.id

        dto = fileset.as_dto()
        self._fileset_dao.add(dto)

        self._add_fileset(fileset)
        dataset = self._add_dataset(dataset)
        return dataset

    def get(self, id: str) -> Dataset:
        "Returns a Dataset with the designated id"
        dataset_dto = self._dataset_dao.get(id)
        fileset_dto = self._fileset_dao.get(dataset_dto.fileset_id)
        fileset = self._fileset.from_dto(fileset_dto)
        dataset = self._dataset.from_dto(dataset_dto)
        dataset.fileset = fileset
        return dataset

    def update(self, dataset: Dataset) -> None:
        """Updates a Dataset in the databases."""
        fileset = dataset.fileset
        self._update_fileset(fileset)
        self._update_dataset(dataset)

    def remove(self, id: str) -> None:
        """Removes a Dataset with id from repository."""
        self._dao.delete(id)

    def exists(self, id: str) -> bool:
        """Returns True if entity with id exists in the repository."""
        return self._dao.exists(id)

    def print(self) -> None:
        """Prints the repository contents as a DataFrame."""
        datasets = self._dao.read_all()
        df = pd.DataFrame.from_dict(data=sources, orient='index', columns=['id', 'name', 'description', 'publisher', 'website', 'url'])
        print(df)

    # -------------------------------------------------------------------------------------------- #
    def _add_dataset(self, dataset: Dataset) -> Dataset:
        dto = dataset.as_dto()
        self._dataset_dao.add(dto)

    def _add_fileset(self, fileset: Fileset) -> Fileset:
        dto = fileset.as_dto()
        self._fileset_dao.add(dto)

    # -------------------------------------------------------------------------------------------- #
    def _update_dataset(self, dataset: Dataset) -> None:
        dto = dataset.as_dto()
        self._dataset_dao.update(dto)

    def _update_fileset(self, fileset: Fileset) -> None:
        dto = fileset.as_dto()
        self._fileset_dao.update(dto)
