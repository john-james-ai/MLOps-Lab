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
# Created    : Friday December 16th 2022 12:28:07 am                                               #
# Modified   : Wednesday December 28th 2022 12:32:53 am                                            #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Dataset Repo Module"""
from recsys.core.entity.base import Entity
from .base import Repo, Context


# ------------------------------------------------------------------------------------------------ #
#                                    DATASET REPOSITORY                                            #
# ------------------------------------------------------------------------------------------------ #
class DatasetRepo(Repo):
    """Repository base class"""

    def __init__(self, context: Context()) -> None:
        super().__init__(context=context)

    def __len__(self) -> int:
        return len(self.get_all())

    def add(self, entity: Entity) -> Entity:
        """Adds an entity to the repository and returns the Entity with the id added."""
        entity = self._context.dataset.create(entity)
        self._context.dataset.save()
        if entity.dataframe_count > 0:
            for name in entity.dataframe_names:
                dataframe = entity.get_dataframe(name)
                # Note: Persist is false because the DataFrames are persisted with the Dataset
                # object.
                self._context.dataframe.create(entity=dataframe, persist=False)
                self._context.dataframe.save()

        return entity

    def get(self, id: str) -> Entity:
        "Returns an entity with the designated id"
        return self._context.dataset.read(id)

    def get_by_name(self, name: str) -> Entity:
        return self._context.dataset.read_by_name(name)

    def get_all(self) -> dict:
        return self._context.dataset.read_all()

    def update(self, entity: Entity) -> None:
        """Updates an entity in the database."""
        self._context.dataset.update(entity)
        self._context.dataset.save()
        if entity.dataframe_count > 0:
            for name in entity.dataframe_names:
                dataframe = entity.get_dataframe(name)
                # Note: Persist is false because the DataFrames are persisted with the Dataset
                # object.
                self._context.dataframe.update(entity=dataframe, persist=False)
                self._context.dataframe.save()

    def remove(self, id: str) -> None:
        """Removes an entity (and its children) from repository."""
        entity = self.get(id)
        if entity.dataframe_count > 0:
            for name in entity.dataframe_names:
                dataframe = entity.get_dataframe(name)
                self._context.dataframe.delete(dataframe.id)
                self._context.dataframe.save()
        self._context.dataset.delete(id)
        self._context.dataset.save()

    def exists(self, id: str) -> bool:
        """Returns True if entity with id exists in the repository."""
        return self._context.dataset.exists(id)

    def print(self) -> None:
        """Prints the repository contents as a DataFrame."""
        datasets = self._context.dataset.read_all()
        for id, dataset in datasets.items():
            print("\n")
            print(dataset)
            if dataset.dataframe_count > 0:
                for name in dataset.dataframe_names:
                    dataframe = dataset.get_dataframe(name)
                    print(dataframe)
