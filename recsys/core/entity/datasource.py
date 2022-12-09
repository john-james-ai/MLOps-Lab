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
# Modified   : Friday December 9th 2022 09:09:02 am                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
from datetime import datetime

from recsys.core.dal.dto import DataSourceDTO
from .base import Entity
# ------------------------------------------------------------------------------------------------ #


class DataSource(Entity):
    """A Source of Data for the Recsys Project

    Args:
        name (str): Name of the source
        publisher (str): The publisher of the data source
        description (str): Description of the source
        website (str): The base website for the source.
        url (str): The download link
    """
    def __init__(self, name: str, publisher: str, website: str, url: str, description: str = None) -> None:
        super().__init__(name=name, description=description)
        self._website = website
        self._publisher = publisher
        self._url = url

        self._validate()

    def __str__(self) -> str:
        return f"\n\nDatasource Id: {self._id}\n\tName: {self._name}\n\tPublisher: {self._publisher}\n\tDescription: {self._description}\n\tWebsite: {self._website}\n\tURL: {self._url}\n\tCreated: {self._created}\n\tModified: {self._modified}"

    def __repr__(self) -> str:
        return f"{self._id}, {self._name}, {self._description}, {self._website}, {self._created}, {self._modified}"

    def __eq__(self, other) -> bool:
        if isinstance(other, DataSource):
            return (
                self._name == other.name
                and self._publisher == other._publisher
                and self._description == other.description
                and self._website == other.website
                and self._url == other.url
            )
        else:
            return False

    # ------------------------------------------------------------------------------------------------ #
    @property
    def publisher(self) -> str:
        return self._publisher

    @publisher.setter
    def publisher(self, publisher: str) -> None:
        self._publisher = publisher
        self._modified = datetime.now()

    # ------------------------------------------------------------------------------------------------ #
    @property
    def website(self) -> str:
        return self._website

    @website.setter
    def website(self, website: str) -> None:
        self._website = website
        self._modified = datetime.now()

    # ------------------------------------------------------------------------------------------------ #
    @property
    def url(self) -> str:
        return self._url

    @url.setter
    def url(self, url: str) -> None:
        self._url = url
        self._modified = datetime.now()

    # ------------------------------------------------------------------------------------------------ #
    def as_dto(self) -> DataSourceDTO:
        return DataSourceDTO(
            id=self._id,
            name=self._name,
            publisher=self._publisher,
            description=self._description,
            website=self._website,
            url=self._url,
            created=self._created,
            modified=self._modified,
        )

    # ------------------------------------------------------------------------------------------------ #
    def _from_dto(self, dto: DataSourceDTO) -> Entity:
        super().__init__(name=dto.name, description=dto.description)
        self._id = dto.id
        self._name = dto.name
        self._publisher = dto.publisher
        self._description = dto.description
        self._website = dto.website
        self._url = dto.url
        self._created = dto.created
        self._modified = dto.modified
        self._validate()
