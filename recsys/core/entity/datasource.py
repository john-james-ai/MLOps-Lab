#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/entity/datasource.py                                                   #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday December 8th 2022 04:26:05 am                                              #
# Modified   : Saturday December 10th 2022 08:47:17 pm                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""DataSource Entity Module"""
from recsys.core.dal.dto import DataSourceDTO
from .fileset import Fileset
from .base import Entity

import recsys
# ------------------------------------------------------------------------------------------------ #


class DataSource(Entity):
    """A Source of Data for the Recsys Project

    Args:
        name (str): Name of the datasource
        publisher (str): The publisher of the data datasource
        description (str): Description of the datasource
        website (str): The base website for the datasource.
    """
    def __init__(self, name: str, publisher: str, website: str, description: str = None) -> None:
        super().__init__(name=name, description=description)
        self._website = website
        self._publisher = publisher
        self._filesets = []

        self._validate()

    def __str__(self) -> str:
        return f"\n\nDatasource Id: {self._id}\n\tName: {self._name}\n\tPublisher: {self._publisher}\n\tDescription: {self._description}\n\tWebsite: {self._website}\n\tCreated: {self._created}\n\tModified: {self._modified}"

    def __repr__(self) -> str:
        return f"{self._id}, {self._name},  {self._publisher}, {self._description}, {self._website}, {self._created}, {self._modified}"

    def __eq__(self, other) -> bool:
        if isinstance(other, DataSource):
            return (
                self._name == other.name
                and self._publisher == other._publisher
                and self._description == other.description
                and self._website == other.website
            )
        else:
            return False

    # ------------------------------------------------------------------------------------------------ #
    @property
    def publisher(self) -> str:
        return self._publisher

    # ------------------------------------------------------------------------------------------------ #
    @property
    def website(self) -> str:
        return self._website

    # ------------------------------------------------------------------------------------------------ #
    def add_fileset(self, fileset: Fileset) -> None:
        self._filesets.append(fileset)

    # ------------------------------------------------------------------------------------------------ #
    def as_dto(self) -> DataSourceDTO:
        return DataSourceDTO(
            id=self._id,
            name=self._name,
            publisher=self._publisher,
            description=self._description,
            website=self._website,
            filesets=self._filesets,
            created=self._created,
            modified=self._modified,
        )

    # ------------------------------------------------------------------------------------------------ #
    def _from_dto(self, dto: DataSourceDTO) -> Entity:
        super().__init__(name=dto.name, description=dto.description)
        self._id = dto.id
        self._publisher = dto.publisher
        self._website = dto.website
        self._filesets = dto.filesets
        self._created = dto.created
        self._modified = dto.modified
        self._validate()

    # ------------------------------------------------------------------------------------------------ #
    def _validate(self) -> None:
        super()._validate()

        if self._name not in recsys.SOURCES:
            msg = f"Error instantiating {self.__class__.__name__}. Attribute 'source' is invalid. Must be one of {recsys.SOURCES}."
            self._logger.error(msg)
            raise ValueError(msg)
