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
# Modified   : Thursday December 1st 2022 01:29:05 am                                              #
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
from urllib.request import urlopen

from recsys.config.workflow import OperatorParams

from recsys.config.base import DatasetGroup
from recsys.core.workflow.pipeline import Context
from recsys.core.dal.dataset import Dataset
from recsys.core.services.repository import repository
from recsys.core.utils.data import clustered_sample

# ------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #

# ================================================================================================ #
#                                 COMMON OPERATORS                                                 #
# ================================================================================================ #


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
        step_params (StepPO): Parameters that provide identity and control behavior.
        input_params (InputPO): Input data
        output_params (OutputPO] Output Data

    Returns: Output Dataset Object
    """

    def __init__(
        self,
        step_params: OperatorParams = None,
        input_params: OperatorParams = None,
        output_params: OperatorParams = None,
    ) -> None:
        step_params = step_params.as_dict()
        super().__init__(**step_params, input_params=input_params, output_params=output_params)
        self._started = None
        self._ended = None
        self._duration = None
        self.input_data = None

    @property
    def input_data(self) -> pd.DataFrame:
        return self._input_data

    @input_data.setter
    def input_data(self, input_data: pd.DataFrame) -> None:
        self._input_data = input_data

    @repository
    def run(
        self, data: Dataset = None, context: Context = None, *args, **kwargs
    ) -> Union[Dataset, DatasetGroup]:
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

        elif isinstance(data, DatasetGroup):
            datasets = data.get_datasets()

            for k, v in datasets.items():
                v.cost = self._duration
                datasets[k] = v
            data = datasets

        else:
            msg = "Output is invalid. Not a dictionary nor a Dataset."
            logger.error(msg)
            raise TypeError(msg)
        return data


# ------------------------------------------------------------------------------------------------ #
#                                         DOWNLOADER                                               #
# ------------------------------------------------------------------------------------------------ #


class Downloader(Operator):
    """Downloads Dataset from a website using urllib

    Args:
        url (str): The URL to the web resource
        destination (str): The file into which the data will be downloaded.
    """

    def __init__(
        self,
        url: str,
        destination: str,
        force: bool = False,
        name: str = None,
        description: str = None,
    ) -> None:

        name = self.__class__.__name__.lower() if name is None else name
        description = self.__class__.__name__ + " from " + url + " to " + destination

        super().__init__(
            name=name,
            description=description,
            url=url,
            destination=destination,
            force=force,
        )

    # @profiler
    def execute(self, data: pd.DataFrame = None, context: Context = None, *args, **kwargs) -> None:
        """Download file."""

        if self._proceed():
            os.makedirs(os.path.dirname(self.destination), exist_ok=True)

            zipresp = urlopen(self.url)
            # Create a new file on the hard drive
            tempzip = open("/tmp/tempfile.zip", "wb")
            # Write the contents of the downloaded file into the new file
            tempzip.write(zipresp.read())
            # Close the newly-created file
            tempzip.close()
            # Re-open the newly-created file with ZipFile()
            zf = ZipFile("/tmp/tempfile.zip")
            # Extract its contents into <extraction_path>
            # note that extractall will automatically create the path
            zf.extractall(path=self.destination)
            # close the ZipFile instance
            zf.close()

    def _proceed(self) -> bool:
        if self.force:
            return True
        elif os.path.exists(self.destination):
            return False
        else:
            return True


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

    # @profiler
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


class EliFpiZ(Operator):
    """Unzips a ZipFile archive

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

    # @profiler
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
#                                          DataGenerator                                           #
# ------------------------------------------------------------------------------------------------ #


class DataGenerator(Operator):
    """Generates data from an input source to a destination of a designated sample size and format.

    Args:
        name (str): Name for the instance
        description (str): Description for the instance.
        infilepath (str): Path to file being copied
        outfilepath (str): The destination filepath
        index_col (int, bool): The column to use for the index. Default = False
        encoding (str): Encoding to use when reading the csv file. Default = utf-8
        low_memory (bool): Whether to process file in chunks, resulting in lower memory use. Default = False
        use_cols (list): The columns to read from the CSV file.
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
        use_cols: list,
        index_col: bool = False,
        encoding: str = "utf-8",
        low_memory: bool = False,
        clustered: bool = False,
        clustered_by: str = None,
        frac: float = None,
        random_state: int = None,
        name: str = None,
        description: str = None,
        force: bool = False,
    ) -> None:

        super().__init__(
            name=name,
            description=description,
            infilepath=infilepath,
            outfilepath=outfilepath,
            use_cols=use_cols,
            index_col=index_col,
            encoding=encoding,
            low_memory=low_memory,
            clustered=clustered,
            clustered_by=clustered_by,
            frac=frac,
            random_state=random_state,
            force=force,
        )

    # @profiler
    def execute(self, data: pd.DataFrame = None, context: Context = None, *args, **kwargs) -> None:
        """Executes the operation

        Args:
            context (Context): Context object containing the name
                and description of Pipeline, and the io object as well.
        """
        if self._proceed():
            io = context.io
            data = io.read(
                self.infilepath,
                index_col=self.index_col,
                usecols=self.usecols,
                low_memory=self.low_memory,
                encoding=self.encoding,
            )
            if self.frac == 1.0:
                pass
            elif self.clustered:
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
            logger.info("DataGenerator skipped as {} already exists.".format(outfilename))
            return False
        else:
            return True
