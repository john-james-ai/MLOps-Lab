#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/entity/dataset.py                                                      #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday December 4th 2022 07:32:54 pm                                                #
# Modified   : Monday December 5th 2022 03:01:09 am                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Dataset Entity Module"""
import os
import inspect
import dotenv
from datetime import datetime
import pandas as pd

from recsys import SOURCES, WORKSPACES, STAGES
from recsys.core.dal.dataset import DatasetDTO
from .base import Entity

# ------------------------------------------------------------------------------------------------ #


class Dataset(Entity):
    """Dataset encapsulates tabular data, metadata, and access behaviors for data used in this package.

    Args:
        id (int): Unique integer identifier for the Dataset object.
        name (str): Short, yet descriptive lowercase name for Dataset object.
        description (str): Describes the Dataset object.
        source (str): The data source
        workspace (str): One of ['prod', 'dev', 'test']
        stage (str): The stage of the data processing lifecycle to which the Dataset belongs.
        version (int): Version is initialized at 1 and bumped by the repo if the Dataset object exists.*
        data (pd.DataFrame): A pandas DataFrame containing the data.
        cost (int): Time in seconds required to produce the Dataset object.
        nrows (int): The number of rows in the Dataset
        ncols (int): The number of columns in the Dataset
        null_counts (int): Number of null values in the Dataset
        memory_size_mb (int): The number of megabytes of memory the DataFrame consumes.
        filepath (str): The location for persistence
        task_id (int): The step within a pipeline task that produced the Dataset object.
        creator (str): The class of the object that created the Dataset.
        created (datetime): Datetime the Dataset was created
        modified (datetime): Datetime the Dataset was modified.


    """

    def __init__(
        self,
        id: int = None,
        name: str = None,
        description: str = None,
        source: str = None,
        workspace: str = None,
        stage: str = None,
        version: int = None,
        data: pd.DataFrame = None,
        cost: int = None,
        nrows: int = None,
        ncols: int = None,
        null_counts: int = None,
        memory_size_mb: int = None,
        filepath: str = None,
        task_id: int = None,
        creator: str = None,
        created: datetime = None,
        modified: datetime = None,
    ) -> None:
        self._id = id
        self._name = name
        self._description = description
        self._source = source
        self._workspace = workspace
        self._stage = stage
        self._version = version
        self._data = data
        self._cost = cost
        self._nrows = nrows
        self._ncols = ncols
        self._null_counts = null_counts
        self._memory_size_mb = memory_size_mb
        self._filepath = filepath
        self._task_id = task_id
        self._creator = creator
        self._created = created
        self._modified = modified
        super().__init__()

        # Set metadata
        self._set_operational_metadata()
        if self._data is not None:
            self._set_data_metadata()

    def __str__(self) -> str:
        return f"\n\nDataset Id: {self._id}\n\tName: {self._name}\n\tDescription: {self._description}\n\tData Source: {self._source}\n\tWorkspace: {self._workspace}\n\tStage: {self._stage}\n\tVersion: {self._version}\n\tData: {self._data}\n\tCost: {self._cost}\n\tNrows: {self._nrows}\n\tNcols: {self._ncols}\n\tNull_Counts: {self._null_counts}\n\tMemory_Size_Mb: {self._memory_size_mb}\n\tFilepath: {self._filepath}\n\tStep_Id: {self._task_id}\n\tCreator: {self._creator}\n\tCreated: {self._created}\n\tModified: {self._modified}"

    def __repr__(self) -> str:
        return f"{self._id},{self._name},{self._description},{self._source},{self._workspace},{self._stage},{self._version},{self._data},{self._cost},{self._nrows},{self._ncols},{self._null_counts},{self._memory_size_mb},{self._filepath},{self._task_id},{self._creator},{self._created},{self._modified}"

    def __eq__(self, other) -> bool:
        """Compares two Datasets for equality.
        Equality is defined by the extent to which the data and metadata are equal. Operational
        metadata, such as creator, created, and version are excluded from the comparison.
        Args:
            other (Dataset): The Dataset object to compare.
        """

        if isinstance(other, Dataset):
            return (
                self._id == other.id
                and self._name == other.name
                and self._description == other.description
                and self._source == other.source
                and self._workspace == other.workspace
                and self._stage == other.stage
                and self._data.equals(other.data)
                and self._cost == other.cost
                and self._nrows == other.nrows
                and self._ncols == other.ncols
                and self._null_counts == other.null_counts
                and self._memory_size_mb == other.memory_size_mb
                and self._filepath == other.filepath
            )

    # ------------------------------------------------------------------------------------------------ #
    # Id var and variables assigned at instantiation
    @property
    def id(self) -> int:
        return self._id

    @id.setter
    def id(self, id: int) -> None:
        if self._id is None:
            self._id = id
            self._modified = datetime.now()
        elif not self._id == id:
            msg = "Item reassignment is not supported for the 'id' member."
            self._logger.error(msg)
            raise TypeError(msg)

    @property
    def name(self) -> str:
        return self._name

    @property
    def description(self) -> str:
        return self._description

    @description.setter
    def description(self, description: str) -> None:
        self._description = description
        self._modified = datetime.now()

    @property
    def source(self) -> str:
        return self._source

    @property
    def workspace(self) -> str:
        return self._workspace

    @property
    def stage(self) -> str:
        return self._stage

    @property
    def version(self) -> str:
        return self._version

    @version.setter
    def version(self, version: int) -> None:
        self._version = version
        self._modified = datetime.now()

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
            self._logger.error(msg)
            raise TypeError(msg)

    @property
    def cost(self) -> str:
        return self._cost

    @cost.setter
    def cost(self, cost: int) -> None:
        self._cost = cost
        self._modified = datetime.now()

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

    @property
    def filepath(self) -> str:
        return self._filepath

    @filepath.setter
    def filepath(self, filepath: int) -> None:
        self._filepath = filepath
        self._modified = datetime.now()

    @property
    def task_id(self) -> int:
        return self._task_id

    @property
    def creator(self) -> str:
        return self._creator

    @property
    def created(self) -> datetime:
        return self._created

    @property
    def modified(self) -> datetime:
        return self._modified

    # ------------------------------------------------------------------------------------------------ #
    # Validation logic

    def validate(self) -> None:
        def announce_and_raise_value_error(msg: str) -> None:
            self._logger.error(msg)
            raise ValueError(msg)

        def announce_and_raise_type_error(msg: str) -> None:
            self._logger.error(msg)
            raise TypeError(msg)

        if self._name is None:
            msg = "Name is a required value for Dataset objects."
            announce_and_raise_value_error(msg)

        if self._workspace not in WORKSPACES:
            msg = f"Workspace is {self._workspace} is invalid. Must be one of {WORKSPACES}."
            announce_and_raise_value_error(msg)

        if self._source not in SOURCES:
            msg = f"Workspace is {self._source} is invalid. Must be one of {SOURCES}."
            announce_and_raise_value_error(msg)

        if self._stage not in STAGES:
            msg = f"Workspace is {self._source} is invalid. Must be one of {SOURCES}."
            announce_and_raise_value_error(msg)

        if not isinstance(self._data, pd.DataFrame):
            msg = f"The data member must be a pandas Dataframe, not {type(self._data)}."
            announce_and_raise_type_error(msg)

        if not isinstance(self._cost, int):
            msg = f"The cost member must be an integer, not {type(self._cost)}."
            announce_and_raise_type_error(msg)

        if not isinstance(self._nrows, int):
            msg = f"The nrows member must be an integer, not {type(self._nrows)}."
            announce_and_raise_type_error(msg)

        if not isinstance(self._ncols, int):
            msg = f"The ncols member must be an integer, not {type(self._ncols)}."
            announce_and_raise_type_error(msg)

        if not isinstance(self._null_counts, int):
            msg = f"The null_counts member must be an integer, not {type(self._null_counts)}."
            announce_and_raise_type_error(msg)

        if not isinstance(self._memory_size_mb, int):
            msg = f"The memory_size_mb member must be an integer, not {type(self._memory_size_mb)}."
            announce_and_raise_type_error(msg)

    # ------------------------------------------------------------------------------------------------ #
    # Data Access methods

    def info(self) -> None:
        self._data.info(verbose=True, memory_usage=True, show_counts=True)

    def head(self, n: int = 5) -> pd.DataFrame:
        return self._data.head(n)

    def tail(self, n: int = 5) -> pd.DataFrame:
        return self._data.tail(n)

    # ------------------------------------------------------------------------------------------------ #
    def _set_operational_metadata(self) -> None:
        dotenv.load_dotenv()
        self._workspace = self._workspace or os.getenv("WORKSPACE")

        self._description = self._description or f"{self.__class__.__name__}.{self._name}"
        self._created = self._created or datetime.now()
        self._modified = datetime.now()
        stack = inspect.stack()
        try:
            self._creator = self._creator or stack[3][0].f_locals["self"].__class__.__name__
        except KeyError:
            self._creator = "Not Designated"

    # ------------------------------------------------------------------------------------------------ #
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
            name=self._name,
            description=self._description,
            source=self._source,
            workspace=self._workspace,
            stage=self._stage,
            version=self._version,
            cost=self._cost,
            nrows=self._nrows,
            ncols=self._ncols,
            null_counts=self._null_counts,
            memory_size_mb=self._memory_size_mb,
            filepath=self._filepath,
            task_id=self._task_id,
            creator=self._creator,
            created=self._created,
            modified=self._modified,
        )

    # ------------------------------------------------------------------------------------------------ #
    def from_dto(self, dto: DatasetDTO) -> None:
        self._id = dto.id
        self._name = dto.name
        self._description = dto.description
        self._source = dto.source
        self._workspace = dto.workspace
        self._stage = dto.stage
        self._version = dto.version
        self._cost = dto.cost
        self._nrows = dto.nrows
        self._ncols = dto.ncols
        self._null_counts = dto.null_counts
        self._memory_size_mb = dto.memory_size_mb
        self._filepath = dto.filepath
        self._task_id = dto.task_id
        self._creator = dto.creator
        self._created = dto.created
        self._modified = datetime.now()
        self._set_operational_metadata()
