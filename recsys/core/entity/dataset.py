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
# Modified   : Friday December 2nd 2022 02:33:33 am                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Data Repository Module"""
import os
import dotenv
from datetime import datetime
import pandas as pd

from dependency_injector.wiring import Provide, inject

from recsys.config import IMMUTABLE_TYPES, SEQUENCE_TYPES
from .base import Entity
from ..dal.dto import DatasetDTO
from recsys.core.services.io import IOService
from .containers import IO

# ------------------------------------------------------------------------------------------------ #


class Dataset(Entity):
    """Dataset encapsulates tabular data, metadata, and access behaviors for data used in this package.

    Args:
        id (int): Only set during reconstruction of the Dataset by the repo.
        source (str): Data source
        env (str): The environment, one of ['dev', 'prod', 'test']
        name (str): Required. Short lowercase alphabetic label for the dataset.
        description (str): Optional. Describes the Dataset
        data (pd.DataFrame): Required. The data payload.
        stage (str): Required. One of ['raw', 'interim', 'final']
        version (int): Required. Initializes at 1 by default, but may be incremented by Repo.
        cost (int): Required. The number of seconds the Operator took to create the Dataset.

        creator (str): Required. Class name for the operator that is creating the Dataset.
        created (datetime): Required. Datetime the Dataset was created.

    """

    @inject
    def __init__(
        self,
        source: str = "movielens25m",
        name: str = None,
        description: str = None,
        data: pd.DataFrame = None,
        stage: str = None,
        version: int = 1,
        task_id: int = None,
        step_id: int = None,
        io: IOService = Provide[IO.io],
        *args,
        **kwargs,
    ) -> None:

        self._source = source
        self._name = name
        self._description = description
        self._data = data
        self._stage = stage
        self._version = version or 1
        self._task_id = task_id
        self._step_id = step_id

        self._io = io

        self._id = None
        self._env: str = (None,)
        self._cost = None
        self._nrows = None
        self._ncols = None
        self._null_counts = None
        self._memory_size_mb = None
        self._filepath = None
        self._created = None
        self._is_archived = None
        self._archived = None

        self._set_metadata()

        super().__init__()

    def __str__(self) -> str:
        return f"\n\nDataset ID: {self._id}\n\tDatasource: {self._source}\n\tEnvironment: {self._env}\n\tName: {self._name}\n\tDescription: {self._description}\n\tStage: {self._stage}\
            \n\tVersion: {self._version}\n\tCost: {self._cost}\n\tNrows: {self._nrows}\n\tNcols: {self._ncols}\n\tNull Counts: {self._null_counts}\
            \n\tMemory Size: {self._memory_size_mb}\n\tFilepath: {self._filepath}\n\tTask Id: {self._task_id}\n\tStep Id: {self._step_id}\n\tCreated: {self._created}\
            \n\tIs Archived: {self._is_archived}\n\tArchived: {self._archived}"

    def __repr__(self) -> str:
        return f"Dataset({self._id}, {self._source}, {self._env}, {self._name}, {self._description}, {self._stage}, {self._version}, {self._cost}, {self._nrows}, {self._ncols}, {self._null_counts}, {self._memory_size_mb}, {self._filepath}, {self._task_id}, {self._step_id}, {self._created}, {self._is_archived}, {self._archived},"

    def __eq__(self, other) -> bool:
        """Compares two Datasets for equality.

        Equality is defined by the extent to which the data and metadata are equal. Operational
        metadata, such as creator, created, and version are not considered.

        Args:
            other (Dataset): The Dataset object to compare.
        """

        if isinstance(other, Dataset):
            return (
                self._source == other.source
                and self._env == other.env
                and self._name == other.name
                and self._description == other.description
                and self._data.equals(other.data)
                and self._stage == other.stage
                and self._cost == other.cost
                and self._nrows == other.nrows
                and self._ncols == other.ncols
                and self._null_counts == other.null_counts
                and self._memory_size_mb == other.memory_size_mb
                and self._filepath == other.filepath
                and self._task_id == other.task_id
                and self._step_id == other.step_id
            )

    # ------------------------------------------------------------------------------------------------ #
    # Id var and variables assigned by the repo
    @property
    def id(self) -> int:
        return self._id

    @id.setter
    def id(self, id: int) -> None:
        self._id = id

    # ------------------------------------------------------------------------------------------------ #
    # Id var and variables assigned at instantiation
    @property
    def source(self) -> str:
        return self._source

    @property
    def env(self) -> str:
        return self._env

    @property
    def name(self) -> str:
        return self._name

    @property
    def description(self) -> str:
        return self._description

    @property
    def stage(self) -> str:
        return self._stage

    @property
    def task_id(self) -> str:
        return self._task_id

    @property
    def step_id(self) -> str:
        return self._step_id

    @property
    def created(self) -> datetime:
        return self._created

    # ------------------------------------------------------------------------------------------------ #
    # Variables assigned at instantiation but may be updated by the repo.
    @property
    def version(self) -> str:
        return self._version

    @version.setter
    def version(self, version: int) -> None:
        self._version = version

    # ------------------------------------------------------------------------------------------------ #
    # Variables assigned at instantiation internally

    @property
    def cost(self) -> str:
        return self._cost

    @cost.setter
    def cost(self, cost: int) -> None:
        self._cost = cost

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
    def filepath(self) -> str:
        return self._filepath

    @filepath.setter
    def filepath(self, filepath: int) -> None:
        self._filepath = filepath

    @property
    def is_archived(self) -> str:
        return self._is_archived

    @is_archived.setter
    def is_archived(self, is_archived: int) -> None:
        self._is_archived = is_archived

    @property
    def archived(self) -> str:
        return self._archived

    @archived.setter
    def archived(self, archived: int) -> None:
        self._archived = archived

    # ------------------------------------------------------------------------------------------------ #
    # Variables set by the operator and by the repo
    @property
    def data(self) -> pd.DataFrame:
        return self._data

    @data.setter
    def data(self, data: pd.DataFrame) -> None:
        self._data = data

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

    def validate(self) -> None:

        config = self._io.read("config.yml")

        members = self.as_dict()

        def announce_and_raise(msg: str) -> None:
            self._logger.error(msg)
            raise ValueError(msg)

        # Confirm object has all required parameters
        for k, v in members.items():
            if k in config["DATASET"]["REQUIRED"] and v is None:
                msg = f"Member variable '{k}' is required."
                announce_and_raise(msg)

        # Check validity of key values.
        if self._env not in config["ENVS"]:
            msg = f"Member variable 'env' = {self._env} is invalide. Must be in {config['ENVS']}."
            announce_and_raise(msg)
        if self._source not in config["DATASET"]["SOURCES"]:
            msg = f"Member variable 'source' = {self._source} is invalid. Must be in {config['DATASET']['SOURCES']}."
            announce_and_raise(msg)
        if self._stages not in config["DATASET"]["STAGES"]:
            msg = f"Member variable 'stage' = {self._stage} is invalid. Must be in {config['DATASET']['STAGES']}."
            announce_and_raise(msg)

    # ------------------------------------------------------------------------------------------------ #
    # Data Access methods

    def info(self) -> None:
        self._data.info(verbose=True, memory_usage=True, show_counts=True)

    def head(self, n: int = 5) -> pd.DataFrame:
        return self._data.head(n)

    def tail(self, n: int = 5) -> pd.DataFrame:
        return self._data.tail(n)

    # ------------------------------------------------------------------------------------------------ #
    # Private methods responsible for auto-setting variables.

    def _set_metadata(self) -> None:
        self._set_operational_metadata()
        self._set_data_metadata()

    def _set_operational_metadata(self) -> None:
        dotenv.load_dotenv()
        self._env = os.getenv("ENV")

        self._description = self._description or f"{self.__class__.__name__}.{self._name}"
        self._created = self._created if self._created is not None else datetime.now()

    def _set_data_metadata(self) -> None:
        if isinstance(self._data, pd.DataFrame):
            self._memory_size_mb = float(
                round(self._data.memory_usage(deep=True, index=True).sum() / 1024**2, 2)
            )
            self._nrows = int(self._data.shape[0])
            self._ncols = int(self._data.shape[1])
            self._null_counts = int(self._data.isnull().sum().sum())

    # ------------------------------------------------------------------------------------------------ #
    def as_dto(self) -> DatasetDTO:
        return DatasetDTO(
            id=self._id,
            source=self._source,
            env=self._env,
            name=self._name,
            description=self._description,
            stage=self._stage,
            version=self._version,
            cost=self._cost,
            nrows=self._nrows,
            ncols=self._ncols,
            null_counts=self._null_counts,
            memory_size_mb=self._memory_size_mb,
            filepath=self._filepath,
            task_id=self._task_id,
            step_id=self._step_id,
            created=self._created,
            is_archived=self._is_archived,
            archived=self._archived,
        )

    def from_dto(self, dto: DatasetDTO) -> None:
        self._id = (dto.id,)
        self._source = (dto.source,)
        self._env = (dto.env,)
        self._name = (dto.name,)
        self._description = (dto.description,)
        self._stage = (dto.stage,)
        self._version = (dto.version,)
        self._cost = (dto.cost,)
        self._nrows = (dto.nrows,)
        self._ncols = (dto.ncols,)
        self._null_counts = (dto.null_counts,)
        self._memory_size_mb = (dto.memory_size_mb,)
        self._filepath = (dto.filepath,)
        self._task_id = (dto.task_id,)
        self._step_id = (dto.step_id,)
        self._created = (dto.created,)
        self._is_archived = (dto.is_archived,)
        self._archived = dto.archived
        self.validate()
