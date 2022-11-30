#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /dataset.py                                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday November 14th 2022 01:27:04 am                                               #
# Modified   : Tuesday November 29th 2022 08:01:52 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Data Repository Module"""
import inspect
import logging
from datetime import datetime
import pandas as pd
from typing import Union

from recsys.config.data import IMMUTABLE_TYPES, SEQUENCE_TYPES
from recsys.core.utils.data import clustered_sample


# ------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #


class Dataset:
    """Dataset encapsulates tabular data, metadata, and access behaviors for data used in this package.

    Args:
        id (int): Only set during reconstruction of the Dataset by the repo.
        name (str): Required. Short lowercase alphabetic label for the dataset.
        description (str): Optional. Describes the Dataset
        stage (str): Required. One of ['raw', 'interim', 'final']
        data (pd.DataFrame): Required. The data payload.
        cost (int): Required. The number of seconds the Operator took to create the Dataset.
        version (int): Required. Initializes at 1 by default, but may be incremented by Repo.
        creator (str): Required. Class name for the operator that is creating the Dataset.
        created (datetime): Required. Datetime the Dataset was created.

    """

    def __init__(
        self,
        id: int = None,
        name: str = None,
        description: str = None,
        stage: str = "interim",
        data: pd.DataFrame = None,
        cost: Union[int, float] = None,
        version: int = 1,
        filepath: str = None,
        creator: str = None,
        created: datetime = None,
        *args,
        **kwargs,
    ) -> None:
        self._id = id
        self._name = name
        self._description = description
        self._stage = stage
        self._data = data
        self._cost = cost
        self._version = version
        self._filepath = filepath
        self._created = created if created is not None else datetime.now()
        self._creator = creator

        self._validate()

        # Variables assigned automatically
        self._filepath = None

        self._nrows = None
        self._ncols = None
        self._null_counts = None
        self._memory_size_mb = None

        # State variable
        self._archived = False

        # Set metadata
        self._set_metadata()

    def __str__(self) -> str:
        return f"\n\nDataset ID: {self._id}\n\tName: {self._name}\n\tDescription: {self._description}\n\tStage: {self._stage}\
            \n\tVersion: {self._version}\n\tCost: {self._cost}\n\tNrows: {self._nrows}\n\tNcols: {self._ncols}\n\tNull Counts: {self._null_counts}\
            \n\tMemory Size: {self._memory_size_mb}\n\tFilepath: {self._filepath}\n\tArchived: {self._archived}\n\tCreator: {self._creator}\n\tCreated: {self._created}"

    def __repr__(self) -> str:
        return f"Dataset({self._id}, {self._name}, {self._description}, {self._stage}, {self._version}, {self._cost}, {self._nrows}, {self._ncols}, {self._null_counts}, {self._memory_size_mb}, {self._filepath}, {self._archived}, {self._creator}, {self._created}"

    def __eq__(self, other) -> bool:
        """Compares two Datasets for equality.

        Equality is defined by the extent to which the data and metadata are equal. Operational
        metadata, such as creator, created, and version.

        Args:
            other (Dataset): The Dataset object to compare.
        """

        if isinstance(other, Dataset):
            return (
                self._id == other.id
                and self._name == other.name
                and self._description == other.description
                and self._stage == other.stage
                and self._nrows == other.nrows
                and self._ncols == other.ncols
                and self._null_counts == other.null_counts
                and self._memory_size_mb == other.memory_size_mb
                and self._cost == other.cost
                and self._data.equals(other.data)
            )

    # ------------------------------------------------------------------------------------------------ #
    # Id var and variables assigned at instantiation
    @property
    def id(self) -> int:
        return self._id

    @id.setter
    def id(self, id: int) -> None:
        self._id = id

    @property
    def name(self) -> str:
        return self._name

    @property
    def description(self) -> str:
        return self._description

    @property
    def stage(self) -> str:
        return self._stage

    # ------------------------------------------------------------------------------------------------ #
    # Variables assigned automatically

    @property
    def nrows(self) -> int:
        return self._nrows

    @property
    def ncols(self) -> int:
        return self._ncols

    @property
    def null_counts(self) -> int:
        return self._null_counts

    @property
    def memory_size_mb(self) -> int:
        return self._memory_size_mb

    # ------------------------------------------------------------------------------------------------ #
    # Variables assigned or updated by the repo
    @property
    def version(self) -> str:
        return self._version

    @version.setter
    def version(self, version: int) -> None:
        self._version = version

    @property
    def filepath(self) -> str:
        return self._filepath

    @filepath.setter
    def filepath(self, filepath: int) -> None:
        self._filepath = filepath

    @property
    def archived(self) -> str:
        return self._archived

    @archived.setter
    def archived(self, archived: int) -> None:
        self._archived = archived

    # ------------------------------------------------------------------------------------------------ #
    # Variables set by the operator
    @property
    def created(self) -> datetime:
        return self._created

    @property
    def creator(self) -> str:
        return self._creator

    @property
    def data(self) -> pd.DataFrame:
        return self._data

    @data.setter
    def data(self, data: pd.DataFrame) -> None:
        if self._data is None:
            self._data = data
            self._set_data_metadata()
        else:
            msg = (
                f"The 'data' attribute on  Dataset {self._id} does not support item re-assignment."
            )
            logger.error(msg)
            raise TypeError(msg)

    @property
    def cost(self) -> str:
        return self._cost

    @cost.setter
    def cost(self, cost: int) -> None:
        if self._cost is None:
            self._cost = cost
        else:
            msg = (
                f"The 'cost' attribute on  Dataset {self._id} does not support item re-assignment."
            )
            logger.error(msg)
            raise TypeError(msg)

    # ------------------------------------------------------------------------------------------------ #
    # Archive related methods
    def archive(self) -> None:
        self._archived = True

    def restore(self) -> None:
        self._archived = False

    # ------------------------------------------------------------------------------------------------ #
    # Exports object as dictionary sans the DataFrame.
    def as_dict(self) -> dict:
        """Returns a dictionary representation of the the Config object."""
        return {k.lstrip("_"): self._export_config(v) for k, v in self.__dict__.items()}

    @classmethod
    def _export_config(cls, v):
        """Returns v with Configs converted to dicts, recursively."""
        if isinstance(v, IMMUTABLE_TYPES):
            return v
        elif isinstance(v, SEQUENCE_TYPES):  # pragma: no cover
            return type(v)(map(cls._export_config, v))
        elif isinstance(v, datetime):
            return v.strftime("%H:%M:%S on %m/%d/%Y")
        elif isinstance(v, dict):  # pragma: no cover
            return v
        else:
            """Do nothing"""

    # ------------------------------------------------------------------------------------------------ #
    # Validation logic

    def _validate(self) -> None:  # pragma: no cover
        def announce_and_raise(msg: str) -> None:
            logger.error(msg)
            raise ValueError(msg)

        if self._name is None:
            msg = "Name is a required value for Dataset objects."
            announce_and_raise(msg)

    # ------------------------------------------------------------------------------------------------ #
    # Data Access methods

    def info(self) -> None:
        self._data.info(verbose=True, memory_usage=True, show_counts=True)

    def head(self, n: int = 5) -> pd.DataFrame:
        return self._data.head(n)

    def tail(self, n: int = 5) -> pd.DataFrame:
        return self._data.tail(n)

    def sample(self, n: int = 1, frac: float = None, replace: bool = False) -> pd.DataFrame:
        return self._data.sample(n=n, frac=frac, replace=replace)

    def cluster_sample(
        self,
        by: str,
        frac: float = None,
        n: int = None,
        replace: bool = False,
        shuffle: bool = True,
        ignore_index: bool = False,
        random_state: int = None,
    ) -> pd.DataFrame:
        return clustered_sample(
            df=self._data,
            by=by,
            frac=frac,
            n=n,
            replace=replace,
            shuffle=shuffle,
            ignore_index=ignore_index,
            random_state=random_state,
        )

    # ------------------------------------------------------------------------------------------------ #
    # Private methods responsible for auto-setting variables.

    def _set_metadata(self) -> None:
        self._set_operational_metadata()
        self._set_data_metadata()

    def _set_operational_metadata(self) -> None:
        self._description = self._description or f"{self.__class__.__name__}.{self._name}"
        self._created = self._created if self._created is not None else datetime.now()
        stack = inspect.stack()
        try:
            self._creator = (
                self._creator
                if self._creator is not None
                else stack[3][0].f_locals["self"].__class__.__name__
            )
        except KeyError:
            self._creator = "Not Designated"

    def _set_data_metadata(self) -> None:
        if isinstance(self._data, pd.DataFrame):
            self._memory_size_mb = float(
                round(self._data.memory_usage(deep=True, index=True).sum() / 1024**2, 2)
            )
            self._nrows = int(self._data.shape[0])
            self._ncols = int(self._data.shape[1])
            self._null_counts = int(self._data.isnull().sum().sum())
