#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /operators.py                                                                       #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday November 17th 2022 02:51:27 am                                             #
# Modified   : Saturday November 26th 2022 05:58:13 am                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Base and Core Operator Classes."""
import os
from datetime import datetime
import pandas as pd
from abc import ABC, abstractmethod
import logging
from typing import Union, Dict
import shlex
import shutil
import subprocess
from zipfile import ZipFile

from recsys.config.workflow import OperatorParams

from recsys.core.services.profiler import profiler
from recsys.core.workflow.pipeline import Context
from recsys.core.dal.dataset import Dataset
from recsys.core.services.decorator import repository
from recsys.core.utils.data import clustered_sample


# ------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #


class Operator(ABC):
    """Abstract base class for pipeline operators.

    Note: All operator parameters in kwargs are added to the class as attributes.

    Args:
        name (str): The name for the operator that distinguishes it in the pipeline.
        description (str): A description for the operator instance. Optional
    """

    def __init__(self, *args, **kwargs) -> None:
        for k, v in kwargs.items():
            setattr(self, k, v)

    def __str__(self) -> str:
        return f"{self.__class__.__name__}\n\tAttributes: {self.__dict__.items()}"

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}\n\tAttributes: {self.__dict__.items()}"

    @property
    def started(self) -> datetime:
        return self._started

    @property
    def ended(self) -> datetime:
        return self._ended

    @property
    def duration(self) -> int:
        return self._duration

    def run(
        self, data: pd.DataFrame = None, context: Context = None, *args, **kwargs
    ) -> pd.DataFrame:
        data = self._setup(data=data)
        data = self.execute(data=data, context=context)
        data = self._teardown(data=data)
        return data

    @abstractmethod
    def execute(
        self, data: pd.DataFrame = None, context: Context = None, *args, **kwargs
    ) -> pd.DataFrame:
        """Executes the operator logic."""

    def _setup(self, data: pd.DataFrame = None) -> pd.DataFrame:
        self._started = datetime.now()

    def _teardown(self, data: pd.DataFrame = None) -> pd.DataFrame:
        self._ended = datetime.now()
        self._duration = (self._ended - self._started).total_seconds()
        return data


# ------------------------------------------------------------------------------------------------ #
class DatasetOperator(Operator):
    """Base class for operators that interact with Dataset objects in the Dataset Repository.

    Args:
        step_params (StepParams): Parameters associated with the identifying and processing
            the pipeline step.
        input_params (Union[str, int, Dict[int]]): Input parameters may be a string filepath, a
            Dataset Id, or a dictionary of Dataset name/Id pairs.
        output_params (Union]DatasetParams,Dict[str,DatasetParams]). A DatasetParams object or
            a dictionary of DatasetParams objects.

    Attributes:
        input_dataset (Union[Dataset, List[Dataset]). The input Dataset or Datasets is/are
            injected by the repository decorator.

    Returns: Output Dataset Object
    """

    def __init__(
        self,
        step_params: OperatorParams = None,
        input_params: OperatorParams = None,
        output_params: OperatorParams = None,
    ) -> None:
        super().__init__(
            step_params=step_params, input_params=input_params, output_params=output_params
        )
        self._started = None
        self._ended = None
        self._duration = None
        self.input_dataset = None

    @property
    def input_dataset(self) -> Union[Dataset, Dict[str, Dataset]]:
        return self._input_dataset

    @input_dataset.setter
    def input_dataset(self, input_dataset: Union[Dataset, Dict[str, Dataset]]) -> None:
        self._input_dataset = input_dataset

    @repository
    def run(
        self, data: Dataset = None, context: Context = None, *args, **kwargs
    ) -> Union[Dataset, Dict[str, Dataset]]:
        data = self._setup(data=data)
        data = self.execute(data=data, context=context)
        data = self._teardown(data=data)
        return data

    @abstractmethod
    def execute(self, data: Dataset = None, context: Context = None, *args, **kwargs) -> Dataset:
        """Executes the operator logic."""

    def _setup(self, data: Dataset = None) -> Dataset:
        self._started = datetime.now()
        return data

    def _teardown(self, data: Union[Dataset, Dict[str, Dataset]] = None) -> Dataset:
        self._ended = datetime.now()
        self._duration = (self._ended - self._started).total_seconds()
        if isinstance(data, Dataset):
            data.cost = self._duration

        elif isinstance(data, dict):
            for k, v in data.items():
                v.cost = self._duration
                data[k] = v
        else:
            msg = "Output is invalid. Not a dictionary nor a Dataset."
            logger.error(msg)
            raise TypeError(msg)
        return data


# ------------------------------------------------------------------------------------------------ #
#                                     KAGGLE DOWNLOADER                                            #
# ------------------------------------------------------------------------------------------------ #


class KaggleDownloader(Operator):
    """Downloads Dataset from Kaggle using the Kaggle API
    Args:
        kaggle_filepath (str): The filepath for the Kaggle dataset
        destination (str): The folder to which the data will be downloaded.
    """

    def __init__(
        self,
        kaggle_filepath: str,
        destination: str,
        name: str = None,
        description: str = None,
        force: bool = False,
    ) -> None:
        name = self.__class__.__name__ if name is None else name
        description = self.__str__()
        super().__init__(
            name=name,
            description=description,
            kaggle_filepath=kaggle_filepath,
            destination=destination,
            force=force,
        )

        self.command = (
            "kaggle datasets download" + " -d " + self.kaggle_filepath + " -p " + self.destination
        )

    @profiler
    def execute(self, data: pd.DataFrame = None, context: Context = None, *args, **kwargs) -> None:
        """Downloads compressed data via an API using bash"""
        os.makedirs(self.destination, exist_ok=True)
        if self._proceed():
            subprocess.run(shlex.split(self.command), check=True, text=True, shell=False)

    def _proceed(self) -> bool:
        if self.force:
            return True

        else:
            kaggle_filename = os.path.basename(self.kaggle_filepath) + ".zip"
            if os.path.exists(os.path.join(self.destination, kaggle_filename)):
                logger.info("Download skipped as {} already exists.".format(kaggle_filename))
                return False
            else:
                return True


# ------------------------------------------------------------------------------------------------ #
#                                     DEZIPPER - EXTRACT ZIPFILE                                   #
# ------------------------------------------------------------------------------------------------ #


class DeZipper(Operator):
    """Unzipps a ZipFile archive

    Args:
        zipfilepath (str): The path to the Zipfile to be extracted.
        destination (str): The directory into which the zipfiles shall be extracted
        members (list): Optional, list of members to be extracted
        force (bool): If True, unzip will overwrite existing file(s) if present.
    """

    def __init__(
        self,
        zipfilepath: str,
        destination: str,
        members: list = None,
        name: str = None,
        description: str = None,
        force: bool = False,
    ) -> None:
        name = self.__class__.__name__ if name is None else name
        description = self.__str__()
        super().__init__(
            name=name,
            description=description,
            zipfilepath=zipfilepath,
            destination=destination,
            members=members,
            force=force,
        )

    @profiler
    def execute(self, data: pd.DataFrame = None, context: Context = None, *args, **kwargs) -> None:
        os.makedirs(self.destination, exist_ok=True)

        if self._proceed():
            with ZipFile(self.zipfilepath, "r") as zipobj:
                zipobj.extractall(path=self.destination, members=self.members)

    def _proceed(self) -> bool:
        if self.force:
            return True
        elif os.path.exists(os.path.join(self.destination, self.members[0])):
            zipfilename = os.path.basename(self.zipfilepath)
            logger.info("DeZip skipped as {} already exists.".format(zipfilename))
            return False
        else:
            return True


# ------------------------------------------------------------------------------------------------ #
#                                          PICKLER                                                 #
# ------------------------------------------------------------------------------------------------ #


class Pickler(Operator):
    """Converts a file to pickle format and optionally removes the original file.

    Args:
        infilepath (str): Path to file being converted
        outfilepath (str): Path to the converted file
        infile_format(str): The format of the input file
        infile_params (dict): Optional. Dictionary containing additional keyword arguments for reading the infile.
        usecols (list): List of columns to select.
        outfile_params (dict): Optional. Dictionary containing additional keyword arguments for writing the outfile.
        force (bool): If True, overwrite existing file if it exists.
        kwargs (dict): Additional keyword arguments to be passed to io object.
    """

    def __init__(
        self,
        infilepath: str,
        outfilepath: str,
        infile_format: str = "csv",
        usecols: list = [],
        index_col: bool = False,
        encoding: str = "utf-8",
        low_memory: bool = False,
        name: str = None,
        description: str = None,
        force: bool = False,
    ) -> None:
        name = self.__class__.__name__ if name is None else name
        description = self.__str__()

        super().__init__(
            name=name,
            description=description,
            infilepath=infilepath,
            outfilepath=outfilepath,
            infile_format=infile_format,
            usecols=usecols,
            index_col=index_col,
            encoding=encoding,
            low_memory=low_memory,
            force=force,
        )

    @profiler
    def execute(self, data: pd.DataFrame = None, context: Context = None, *args, **kwargs) -> None:
        """Executes the operation

        Args:
            context (Context): Context object containing the name
                and description of Pipeline, and the io object as well.
        """
        if self._proceed():
            io = context.io
            data = io.read(
                filepath=self.infilepath,
                usecols=self.usecols,
                index_col=self.index_col,
                low_memory=self.low_memory,
                encoding=self.encoding,
            )
            os.makedirs(os.path.dirname(self.outfilepath), exist_ok=True)
            io.write(self.outfilepath, data)

    def _proceed(self) -> bool:
        if self.force:
            return True
        elif os.path.exists(self.outfilepath):
            outfilename = os.path.basename(self.outfilepath)
            logger.info("Pickler skipped as {} already exists.".format(outfilename))
            return False
        else:
            return True


# ------------------------------------------------------------------------------------------------ #
#                                           COPY                                                   #
# ------------------------------------------------------------------------------------------------ #


class Copier(Operator):
    """Copies a file from source to destination

    Args:
        infilepath (str): Path to file being copied
        outfilepath (str): The destination filepath
        force (bool): If True, overwrite existing file if it exists.
    """

    def __init__(
        self,
        infilepath: str,
        outfilepath: str,
        name: str = None,
        description: str = None,
        force: bool = False,
    ) -> None:
        name = self.__class__.__name__ if name is None else name
        description = self.__str__()

        super().__init__(
            name=name,
            description=description,
            infilepath=infilepath,
            outfilepath=outfilepath,
            force=force,
        )

    @profiler
    def execute(self, data: pd.DataFrame = None, context: Context = None, *args, **kwargs) -> None:
        """Executes the operation

        Args:
            context (Context): Context object containing the name
                and description of Pipeline, and the io object as well.
        """
        if self._proceed():
            shutil.copy(src=self.infilepath, dst=self.outfilepath)

    def _proceed(self) -> bool:
        if self.force:
            return True
        elif os.path.exists(self.outfilepath):
            outfilename = os.path.basename(self.outfilepath)
            logger.info("Copier skipped as {} already exists.".format(outfilename))
            return False
        else:
            return True


# ------------------------------------------------------------------------------------------------ #
#                                          SAMPLER                                                 #
# ------------------------------------------------------------------------------------------------ #


class Sampler(Operator):
    """Copies a file from source to destination

    Args:
        infilepath (str): Path to file being copied
        outfilepath (str): The destination filepath
        clustered (bool): Conduct clustered sampling if True. Otherwise, simple random sampling.
        clustered_by (str): The column name to cluster by.
        frac (float): The proportion of the data to return as sample.
        random_state (int): The pseudo random seed for reproducibility.
        force (bool): If True, overwrite existing file if it exists.
    """

    def __init__(
        self,
        infilepath: str,
        outfilepath: str,
        clustered: bool = False,
        clustered_by: str = None,
        frac: float = None,
        random_state: int = None,
        name: str = None,
        description: str = None,
        force: bool = False,
    ) -> None:
        name = self.__class__.__name__ if name is None else name
        description = self.__str__()

        super().__init__(
            name=name,
            description=description,
            infilepath=infilepath,
            outfilepath=outfilepath,
            clustered=clustered,
            clustered_by=clustered_by,
            frac=frac,
            random_state=random_state,
            force=force,
        )

    @profiler
    def execute(self, data: pd.DataFrame = None, context: Context = None, *args, **kwargs) -> None:
        """Executes the operation

        Args:
            context (Context): Context object containing the name
                and description of Pipeline, and the io object as well.
        """
        if self._proceed():
            io = context.io
            data = io.read(self.infilepath)
            if self.clustered:
                data = clustered_sample(
                    df=data, by=self.clustered_by, frac=self.frac, random_state=self.random_state
                )
            else:
                data = data.sample(frac=self.frac, random_state=self.random_state)
            io.write(filepath=self.outfilepath, data=data)

    def _proceed(self) -> bool:
        if self.force:
            return True
        elif os.path.exists(self.outfilepath):
            outfilename = os.path.basename(self.outfilepath)
            logger.info("Sampler skipped as {} already exists.".format(outfilename))
            return False
        else:
            return True
