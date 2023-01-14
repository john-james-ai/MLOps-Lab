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
# Created    : Sunday December 4th 2022 07:32:54 pm                                                #
# Modified   : Saturday January 14th 2023 03:02:17 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""DataSourceURL Entity Module"""
from abc import abstractmethod
from typing import Union, Dict
import pandas as pd
from datetime import datetime

from .base import Builder
from recsys.core.entity.base import Entity
from recsys.core.dal.dto import DataSourceURLDTO, DataSourceDTO


# ------------------------------------------------------------------------------------------------ #
#                                   DATASOURCE COMPONENT                                           #
# ------------------------------------------------------------------------------------------------ #
class DataSourceComponent(Entity):
    """Base component class from which DataSourceURL (Leaf) and DataSource (Composite) objects derive."""

    def __init__(self, name: str, description: str = None) -> None:
        super().__init__(name=name, description=description)

    # -------------------------------------------------------------------------------------------- #
    @property
    @abstractmethod
    def is_composite(self) -> str:
        """True for DataSources and False for DataSourceURLs."""

    # -------------------------------------------------------------------------------------------- #
    @abstractmethod
    def as_dto(self) -> Union[Dict[int, DataSourceDTO], Dict[int, DataSourceURLDTO]]:
        """Creates a dto representation of the DataSource Component."""

    # -------------------------------------------------------------------------------------------- #
    def _validate(self) -> None:
        super()._validate()


# ------------------------------------------------------------------------------------------------ #
#                                      DATASOURCE                                                  #
# ------------------------------------------------------------------------------------------------ #
class DataSource(DataSourceComponent):
    """Collection of DataSourceURL objects.

    Args:
        name (str): Short, yet descriptive lowercase name for DataSource object.
        description (str): Describes the DataSource object.
        website (str): The DataSource primary website
    """

    def __init__(
        self,
        name: str,
        website: str,
        description: str = None,
    ) -> None:
        super().__init__(name=name, description=description)
        self._website = website

        self._urls = {}
        self._is_composite = True

        self._validate()

    def __str__(self) -> str:
        return f"DataSource Id: {self._id}\n\tName: {self._name}\n\tDescription: {self._description}\n\tWebsite: {self._website}\n\tCreated: {self._created}\n\tModified: {self._modified}"

    def __repr__(self) -> str:
        return f"{self._id}, {self._name}, {self._description}, {self._website}, {self._created}, {self._modified}"

    def __eq__(self, other: DataSourceComponent) -> bool:
        if self.__class__.__name__ == other.__class__.__name__:
            return (
                self.is_composite == other.is_composite
                and self.name == other.name
                and self.description == other.description
                and self.website == other.website
            )
        else:
            return False

    @property
    def url_count(self) -> int:
        return len(self._urls)

    # -------------------------------------------------------------------------------------------- #
    @property
    def website(self) -> str:
        return self._website

    # -------------------------------------------------------------------------------------------- #
    @website.setter
    def website(self, website: str) -> None:
        self._website = website

    # -------------------------------------------------------------------------------------------- #
    @property
    def is_composite(self) -> str:
        return self._is_composite

    # -------------------------------------------------------------------------------------------- #
    @property
    def urls(self) -> dict:
        return self._urls

    # -------------------------------------------------------------------------------------------- #
    def create_url(
        self, url: str, name: str = None, description: str = None
    ) -> DataSourceComponent:
        name = name or self._name
        description = description or self._description
        return DataSourceURL(name=name, url=url, datasource=self, description=description)

    # -------------------------------------------------------------------------------------------- #
    def add_url(self, url: DataSourceComponent) -> None:
        self._urls[url.name] = url
        self._modified = datetime.now()

    # -------------------------------------------------------------------------------------------- #
    def get_url(self, name) -> None:
        try:
            return self._urls[name]
        except KeyError:
            msg = f"DataSource {self._name} has no url with name = {name}."
            self._logger.error(msg)
            raise FileNotFoundError(msg)

    # -------------------------------------------------------------------------------------------- #
    def get_urls(self) -> pd.DataFrame:
        d = {}
        for name, url in self._urls.items():
            d[name] = url.as_dto().as_dict()
        df = pd.DataFrame.from_dict(data=d, orient="index")
        return df

    # -------------------------------------------------------------------------------------------- #
    def update_url(self, url: DataSourceComponent) -> None:
        self._urls[url.name] = url
        self._modified = datetime.now()

    # -------------------------------------------------------------------------------------------- #
    def remove_url(self, name: str) -> None:
        del self._urls[name]
        self._modified = datetime.now()

    # -------------------------------------------------------------------------------------------- #
    def as_dto(self) -> DataSourceDTO:

        dto = DataSourceDTO(
            id=self._id,
            oid=self._oid,
            name=self._name,
            description=self._description,
            website=self._website,
            created=self._created,
            modified=self._modified,
        )
        return dto


# ------------------------------------------------------------------------------------------------ #
#                                        DATASOURCE                                                #
# ------------------------------------------------------------------------------------------------ #
class DataSourceURL(DataSourceComponent):
    """DataSourceURL encapsulates tabular data, metadata, and access behaviors for data used in this package.

    Args:
        name (str): Short, yet descriptive lowercase name for DataSourceURL object.
        description (str): Describes the DataSourceURL object. Default's to datasource's description if None.
        url (pd.DataSourceURL): Payload in pandas DataSourceURL format.
        datasource (DataSource): The datasource DataSource instance. Optional.

    """

    def __init__(
        self,
        name: str,
        url: str,
        datasource: DataSource,
        description: str = None,
    ) -> None:
        super().__init__(name=name, description=description)

        self._url = url
        self._datasource = datasource
        self._is_composite = False

        self._set_metadata()
        self._validate()

    def __str__(self) -> str:
        return f"DataSourceURL Id: {self._id}\n\tName: {self._name}\n\tDescription: {self._description}\n\tURL: {self._url}\n\tCreated: {self._created}\n\tModified: {self._modified}"

    def __repr__(self) -> str:
        return f"{self._id}, {self._name}, {self._description}, {self._url}, {self._created}, {self._modified}"

    def __eq__(self, other) -> bool:
        """Compares two DataSource for equality.
        DataSource are considered equal solely if their underlying data are equal.

        Args:
            other (DataSourceURL): The DataSourceURL object to compare.
        """

        if isinstance(other, DataSourceURL):
            return self._url == other.url
        else:
            return False

    def __len__(self) -> int:
        return 1

    # -------------------------------------------------------------------------------------------- #
    @property
    def is_composite(self) -> str:
        return self._is_composite

    # -------------------------------------------------------------------------------------------- #
    @property
    def url(self) -> str:
        return self._url

    # -------------------------------------------------------------------------------------------- #
    @url.setter
    def url(self, url: str) -> None:
        self._url = url

    # -------------------------------------------------------------------------------------------- #
    @property
    def parent(self) -> DataSource:
        return self._parent

    # -------------------------------------------------------------------------------------------- #
    @parent.setter
    def parent(self, parent: DataSource) -> None:
        self._parent = parent

    # ------------------------------------------------------------------------------------------------ #
    def as_dto(self) -> DataSourceURLDTO:
        return DataSourceURLDTO(
            id=self._id,
            oid=self._oid,
            name=self._name,
            description=self._description,
            url=self._url,
            parent_id=self._parent.id,
            created=self._created,
            modified=self._modified,
        )

    # ------------------------------------------------------------------------------------------------ #
    def _set_metadata(self) -> None:
        if self._datasource is not None:
            self._description = self._description or self._datasource.description
