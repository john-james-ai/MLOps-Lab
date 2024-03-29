#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Enter Project Name in Workspace Settings                                            #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /mlops_lab/core/repo/dataset.py                                                     #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : Enter URL in Workspace Settings                                                     #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday December 31st 2022 11:14:54 pm                                             #
# Modified   : Tuesday January 24th 2023 08:13:45 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Dataset Repository"""

from mlops_lab.core.entity.base import Entity
from .base import RepoABC
from .context import Context


# ------------------------------------------------------------------------------------------------ #
#                                           REPOSITORY                                             #
# ------------------------------------------------------------------------------------------------ #
class DatasetRepo(RepoABC):
    """Dataset aggregate repository."""

    def __init__(self, context: Context, *args, **kwargs) -> None:
        super().__init__()
        self._context = context
        self._dataset_dao = self._context.get_dao("dataset")
        self._dataframe_dao = self._context.get_dao("dataframe")
        self._oao = self._context.get_oao()

    def __len__(self) -> int:
        return len(self._dataset_dao.read_all())

    def add(self, entity: Entity) -> Entity:
        """Adds an entity to the repository and returns the Entity with the id added."""

        dto = self._dataset_dao.create(entity.as_dto())
        entity.id = dto.id

        for name, dataframe in entity.dataframes.items():
            dataframe.parent = entity
            dto = self._dataframe_dao.create(dto=dataframe.as_dto())
            dataframe.id = dto.id
            entity.update_dataframe(dataframe)

        self._oao.create(entity)
        return entity

    def get(self, id: str) -> Entity:
        "Returns an entity with the designated id"
        result = []
        dto = self._dataset_dao.read(id)
        if dto:
            result = self._oao.read(dto.oid)
        return result

    def get_by_name(self, name: str) -> Entity:
        result = []
        dto = self._dataset_dao.read_by_name(name)
        if dto:
            result = self._oao.read(dto.oid)
        return result

    def get_all(self) -> dict:
        entities = {}
        dtos = self._dataset_dao.read_all()
        for dto in dtos.values():
            entity = self._oao.read(oid=dto.oid)
            if hasattr(entity, "id"):
                entities[entity.id] = entity
        return entities

    def update(self, entity: Entity) -> None:
        """Updates an entity in the database."""
        for dataframe in entity.dataframes.values():
            self._dataframe_dao.update(dataframe.as_dto())

        self._dataset_dao.update(dto=entity.as_dto())  # Update Dataset metadata
        self._oao.update(entity)  # Persist dataset in object storage

    def remove(self, id: str) -> None:
        """Removes an entity (and its children) from repository."""
        dto = self._dataset_dao.read(id)
        dataset = self._oao.read(dto.oid)
        for dataframe in dataset.dataframes.values():
            self._dataframe_dao.delete(dataframe.id)

        self._dataset_dao.delete(id)  # Delete Dataset metadata
        self._oao.delete(dataset.oid)  # Delete dataset from object storage

    def exists(self, id: str) -> bool:
        """Returns True if entity with id exists in the repository."""
        return self._dataset_dao.exists(id)

    def print(self) -> None:
        """Prints the repository contents as a DataFrame."""
        dtos = self._dataset_dao.read_all()
        for dto in dtos.values():
            dataset = self._oao.read(dto.oid)
            print("\n\n")
            print(dataset)
            print(120 * "=")
            print(50 * " ", "DataFrames")
            print(120 * "_")
            dfs = dataset.get_dataframes()
            print(dfs)
