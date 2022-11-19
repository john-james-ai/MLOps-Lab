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
# Modified   : Saturday November 19th 2022 04:29:37 pm                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Data Repository Module"""
import os
import inspect
import logging
from datetime import datetime
from dataclasses import dataclass
import pandas as pd

from recsys.core.base.config import IMMUTABLE_TYPES, SEQUENCE_TYPES, ENVS, STAGES
from recsys.core.utils.data import clustered_sample


# ------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #


def get_id(name: str, env: str, version: int, stage: str) -> str:
    """Formats the unique id for a Dataset, which will ensure uniquely named Datasets within
    version, environment, and stage.

    Args:
        name (str): The name of the Dataset
        env (str): One of 'prod', 'dev', or 'test'
        version (int): One of the currently used data versions
        stage (str): One of 'raw', 'interim' or 'cooked'
    """
    return env + "_" + stage + "_" + name + "_" + str(version)


@dataclass
class DatasetMeta:
    id: str
    name: str
    description: str
    stage: str
    env: str
    version: str
    versioning: bool
    nrows: int
    ncols: int
    null_counts: int
    memory_size: int
    cost: float
    filename: str
    filepath: str
    created: datetime
    creator: str

    def as_dict(self) -> dict:
        """Returns a dictionary representation of the the Config object."""
        return {k: self._export_config(v) for k, v in self.__dict__.items()}

    @classmethod
    def _export_config(cls, v):
        """Returns v with Configs converted to dicts, recursively."""
        if isinstance(v, IMMUTABLE_TYPES):
            return v
        elif isinstance(v, SEQUENCE_TYPES):
            return type(v)(map(cls._export_config, v))
        elif isinstance(v, datetime):
            return v.strftime("%H:%M:%S on %m/%d/%Y")
        elif isinstance(v, dict):
            return {kk: cls._export_config(vv) for kk, vv in v}
        else:
            pass


# ------------------------------------------------------------------------------------------------ #


class Dataset:
    """Dataset encapsulates tabular data, metadata, and access behaviors for data used in this package.

    Args:
        name (str): Short lowercase alphabetic label for the dataset
        data (pd.Dataframe): The data payload
        stage (str): One of 'raw', 'interim', 'cooked'. Defaults to 'interim'.
        env (str): Either 'prod', 'test', or 'dev'. Default is 'dev'.
        version (int): Version of Dataset. Defaults to one (1).

    """

    def __init__(
        self,
        name: str,
        description: str = None,
        stage: str = "interim",
        env: str = "dev",
        version: int = 1,
        versioning: bool = True,
        data: pd.DataFrame = None,
        cost: int = None,
    ) -> None:
        self._name = name
        self._data = data
        self._stage = stage
        self._env = env
        self._version = version
        self._versioning = versioning
        self._description = description
        self._cost = cost

        self._validate(name=self.name, env=self.env, stage=self.stage, version=self.version)

        # Variables assigned automatically
        self._id = None
        self._columns = None
        self._nrows = None
        self._ncols = None
        self._null_counts = None
        self._memory_size = None
        self._created = None
        self._creator = None

        # Variables set by user
        self._description = None
        self._features = None
        self._classes = None
        self._target = None

        # Variables assigned by repository
        self._filepath = None

        # Variables assigned automatically after filepath assignment.
        self._filename = None
        self._saved = None

        # Set metadata
        self._set_metadata()

    def __str__(self) -> str:
        return f"Dataset file name {self._filename}, with filepath {self._filepath}, described as {self._description}"

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(filename={self._filename}, filepath={self._filepath}, description={self._description}, data={self._data.__class__.__name__}"

    # ------------------------------------------------------------------------------------------------ #
    # Id var and variables assigned at instantiation
    @property
    def id(self) -> str:
        return self._id

    @id.setter
    def id(self, id: str) -> None:
        self._id = id

    @property
    def name(self) -> str:
        return self._name

    @property
    def stage(self) -> str:
        return self._stage

    @property
    def env(self) -> str:
        return self._env

    @property
    def versioning(self) -> str:
        return self._versioning

    # ------------------------------------------------------------------------------------------------ #
    # Variables assigned automatically

    @property
    def columns(self) -> list:
        return self._columns

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
    def memory_size(self) -> int:
        return self._memory_size

    @property
    def created(self) -> datetime:
        return self._created

    @property
    def creator(self) -> str:
        return self._creator

    # ------------------------------------------------------------------------------------------------ #
    # Variables set by user
    @property
    def description(self) -> str:
        return self._description

    @description.setter
    def description(self, description: str) -> None:
        self._description = description

    @property
    def features(self) -> int:
        return self._features

    @features.setter
    def features(self, features: int) -> None:
        self._features = features

    @property
    def classes(self) -> int:
        return self._classes

    @classes.setter
    def classes(self, classes: int) -> None:
        self._classes = classes

    @property
    def target(self) -> int:
        return self._target

    @target.setter
    def target(self, target: int) -> None:
        self._target = target

    # ------------------------------------------------------------------------------------------------ #
    # Variables assigned by the repo
    @property
    def filepath(self) -> str:
        return self._filepath

    @filepath.setter
    def filepath(self, filepath: str) -> None:
        self._filepath = filepath
        self._set_file_metadata()

    @property
    def version(self) -> str:
        return self._version

    @version.setter
    def version(self, version) -> None:
        self._version = version

    # ------------------------------------------------------------------------------------------------ #
    # Variables automatically set after filepath is set
    @property
    def filename(self) -> str:
        return self._filename

    @property
    def saved(self) -> str:
        return self._saved

    # ------------------------------------------------------------------------------------------------ #
    # Variables set by the creator
    @property
    def data(self) -> pd.DataFrame:
        return self._data

    @data.setter
    def data(self, data: pd.DataFrame) -> None:
        self._data = data

    @property
    def cost(self) -> str:
        return self._cost

    @cost.setter
    def cost(self, cost: int) -> None:
        self._cost = cost

    # ------------------------------------------------------------------------------------------------ #
    # Validation logic

    def _validate(self, name: str, env: str, stage: str, version: [int, float]) -> None:
        if name is None:
            msg = "Name is a required value for Dataset objects."
            self._announce_and_raise(msg)
        if stage not in STAGES:
            msg = f"Stage {stage} is invalid. Use one of {STAGES}."
            self._announce_and_raise(msg)
        if env not in ENVS:
            msg = f"Environment {env} is invalid. Use one of {ENVS}."
            self._announce_and_raise(msg)
        if not isinstance(version, (int, float)):
            msg = "Version must be numeric (int,float)."
            self._announce_and_raise(msg)

    def _announce_and_raise(self, msg: str) -> None:
        logger.error(msg)
        raise ValueError(msg)

    # ------------------------------------------------------------------------------------------------ #
    # Access methods

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
        self._id = get_id(name=self._name, env=self._env, version=self._version, stage=self._stage)
        self._description = self._description or f"{self.__class__.__name__}.{self._name}"
        self._created = datetime.now()
        stack = inspect.stack()
        try:
            self._creator = stack[2][0].f_locals["self"].__class__.__name__
        except KeyError:
            self._creator = None

    def _set_data_metadata(self) -> None:
        if self._data is not None:
            self._memory_size = self._data.memory_usage(deep=True, index=True).sum()
            self._nrows = self._data.shape[0]
            self._ncols = self._data.shape[1]
            self._columns = self._data.columns
            self._null_counts = self._data.isnull().sum().sum()

    def _set_file_metadata(self) -> None:
        if self._filepath is not None:
            self._filename = os.path.basename(self._filepath)
            self._saved = datetime.now()

    def as_meta(self) -> DatasetMeta:
        m = DatasetMeta(
            id=self._id,
            name=self._name,
            description=self._description,
            stage=self._stage,
            env=self._env,
            version=self._version,
            versioning=self._versioning,
            nrows=self._nrows,
            ncols=self._ncols,
            null_counts=self._null_counts,
            memory_size=self._memory_size,
            cost=self._cost,
            filename=self._filename,
            filepath=self._filepath,
            created=self._created,
            creator=self._creator,
        )
        return m

    def is_equal(self, other) -> bool:
        return (
            self._id == other.id
            and self._name == other.name
            and self._description == other.description
            and self._stage == other.stage
            and self._env == other.env
            and self._version == other.version
            and self._nrows == other.nrows
            and self._ncols == other.ncols
            and self._null_counts == other.null_counts
            and self._memory_size == other.memory_size
            and self._cost == other.cost
            and self._filename == other.filename
            and self._filepath == other.filepath
            and self._created == other.created
            and self._creator == other.creator
        )


@dataclass
class DatasetParams:
    name: str
    stage: str
    env: str
    version: int = 1
    id: str = None
    description: str = None

    def __post_init__(self) -> None:
        self._validate(name=self.name, env=self.env, stage=self.stage, version=self.version)
        self.id = get_id(name=self.name, env=self.env, version=self.version, stage=self.stage)

    def to_dataset(self) -> Dataset:
        return Dataset(
            name=self.name,
            stage=self.stage,
            env=self.env,
            version=self.version,
            description=self.description,
        )

    def _validate(self, name: str, env: str, stage: str, version: [int, float]) -> None:
        if name is None:
            msg = "Name is a required value for Dataset objects."
            self._announce_and_raise(msg)
        if stage not in STAGES:
            msg = f"Stage {stage} is invalid. Use one of {STAGES}."
            self._announce_and_raise(msg)
        if env not in ENVS:
            msg = f"Environment {env} is invalid. Use one of {ENVS}."
            self._announce_and_raise(msg)
        if not isinstance(version, (int, float)):
            msg = "Version must be numeric (int,float)."
            self._announce_and_raise(msg)

    def _announce_and_raise(self, msg: str) -> None:
        logger.error(msg)
        raise ValueError(msg)
