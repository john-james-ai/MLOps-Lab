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
# Modified   : Thursday November 24th 2022 04:38:59 am                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #

import os
from typing import Any, Union, Dict, List
import numpy as np
from datetime import datetime
import pandas as pd
from abc import ABC, abstractmethod
import logging
import shlex
import subprocess
from zipfile import ZipFile

from recsys.config.base import OperatorParams
from recsys.core.services.profiler import profiler
from recsys.core.workflow.pipeline import Context
from recsys.core.dal.dataset import Dataset
from recsys.core.services.repo import repository


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

    @profiler
    @abstractmethod
    def run(self, data: pd.DataFrame = None, context: Context = None, **kwargs) -> Any:
        pass


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
        force: bool = False,
    ) -> None:
        name = self.__class__.__name__.lower()
        super().__init__(
            name=name,
            kaggle_filepath=kaggle_filepath,
            destination=destination,
            force=force,
        )

        self.command = (
            "kaggle datasets download" + " -d " + self.kaggle_filepath + " -p " + self.destination
        )

    def execute(self, *args, **kwargs) -> None:
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
        force: bool = False,
    ) -> None:
        name = self.__class__.__name__.lower()
        super().__init__(
            name=name,
            zipfilepath=zipfilepath,
            destination=destination,
            members=members,
            force=force,
        )

    def execute(self, *args, **kwargs) -> None:
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
        force: bool = False,
    ) -> None:
        super().__init__(
            infilepath=infilepath,
            outfilepath=outfilepath,
            infile_format=infile_format,
            usecols=usecols,
            index_col=index_col,
            encoding=encoding,
            low_memory=low_memory,
            force=force,
        )

    def execute(self, context: Context, *args, **kwargs) -> None:
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


# ================================================================================================ #
#                                      PREPROCESSING                                               #
# ================================================================================================ #
# ------------------------------------------------------------------------------------------------ #
#                                     CREATE DATASET                                               #
# ------------------------------------------------------------------------------------------------ #


class CreateDataset(DatasetOperator):
    """Reads a DataFrame, creates a Dataset and commits it to the repository.

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
        step_params: OperatorParams,
        input_params: OperatorParams,
        output_params: OperatorParams,
    ) -> None:
        super().__init__(step_params, input_params, output_params)

    def execute(self, data: Dataset = None, context: Context = None, *args, **kwargs) -> Dataset:
        """Creates the dataset."""

        io = context
        try:
            data = io.read(self.input_params.filepath)
        except TypeError:
            msg = f"The input_params must be string. Type {type(self.input_params)} is invalid."
            logger.error(msg)
            raise TypeError(msg)
        except FileNotFoundError:
            msg = f"File not found at {self.input_params}."
            logger.error(msg)
            raise FileNotFoundError(msg)

        dataset = Dataset(
            name=self.output_params.name,
            description=self.output_params.description,
            env=self.output_params.env,
            stage=self.output_params.stage,
            version=self.output_params.version,
            data=data,
        )

        return dataset


# ------------------------------------------------------------------------------------------------ #
#                                   TRAIN TEST SPLIT                                               #
# ------------------------------------------------------------------------------------------------ #
class TrainTestSplit(DatasetOperator):
    """Splits the dataset into to training and test sets by user

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

    Returns: Output Dataset Object Dictionary, containing Train and Test Datasets.

    """

    def __init__(
        self,
        step_params: OperatorParams,
        input_params: OperatorParams,
        output_params: OperatorParams,
    ) -> None:
        super().__init__(step_params, input_params, output_params)

    def execute(self, data: Dataset = None, context: Context = None, *args, **kwargs) -> Dataset:

        data = self.input_dataset

        if self.step_params.clustered:

            rng = np.random.default_rng(self.step_params.random_state)

            clusters = data[self.step_params.clustered_by].unique()
            train_set_size = int(len(clusters) * self.step_params.train_proportion)

            train_clusters = rng.choice(
                a=clusters, size=train_set_size, replace=False, shuffle=True
            )
            test_clusters = np.setdiff1d(clusters, train_clusters)

            train_set = data.loc[data[self.step_params.clustered_by].isin(train_clusters)]
            test_set = data.loc[data[self.step_params.clustered_by].isin(test_clusters)]

        else:
            # Get all indices
            index = np.array(data.index.to_numpy())

            # Split the training set by the train proportion
            train_set = data.sample(
                frac=self.step_params.train_proportion,
                replace=False,
                axis=0,
                random_state=self.step_params.random_state,
            )

            # Obtain training indices and perform setdiff to get test indices
            train_idx = train_set.index
            test_idx = np.setdiff1d(index, train_idx)

            # Extract test data
            test_set = data.loc[test_idx]

        # Create train and test Dataset objects.
        train_params = self.output_params["train"]().as_dict()
        test_params = self.output_params["test"]().as_dict()

        train = Dataset(**train_params, data=train_set)
        test = Dataset(**test_params, data=test_set)

        logger.debug(f"\n\nTrain: \n{train}")
        logger.debug(f"\n\nTest: \n{test}")

        result = {"train": train, "test": test}

        return result


# ------------------------------------------------------------------------------------------------ #
#                                      RATINGS ADJUSTER                                            #
# ------------------------------------------------------------------------------------------------ #


class RatingsAdjuster(DatasetOperator):
    """Centers the ratings by subtracting the users average rating from each of the users ratings.

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
        step_params: OperatorParams,
        input_params: OperatorParams,
        output_params: OperatorParams,
    ) -> None:
        super().__init__(step_params, input_params, output_params)

    def execute(self, data: Dataset = None, context: Context = None, *args, **kwargs) -> Dataset:

        data = self.input_dataset

        data["adj_rating"] = data["rating"].sub(data.groupby("userId")["rating"].transform("mean"))

        params = self.output_params.as_dict()

        dataset = Dataset(**params, data=data)

        return dataset


# ------------------------------------------------------------------------------------------------ #
#                                     USER AVERAGE SCORES                                          #
# ------------------------------------------------------------------------------------------------ #


class User(DatasetOperator):
    """Computes and stores average rating for each user.

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

    Returns Dataset
    """

    def __init__(
        self,
        step_params: OperatorParams,
        input_params: OperatorParams,
        output_params: OperatorParams,
    ) -> None:
        super().__init__(step_params, input_params, output_params)

    def execute(self, data: Dataset = None, context: Context = None, *args, **kwargs) -> Dataset:

        data = self.input_dataset

        user_average_ratings = data.groupby("userId")["rating"].mean().reset_index()
        user_average_ratings.columns = ["userId", "mean_rating"]

        params = self._output_params.as_dict()

        dataset = Dataset(**params, data=user_average_ratings)

        return dataset


# ------------------------------------------------------------------------------------------------ #
#                                               PHI                                                #
# ------------------------------------------------------------------------------------------------ #
class Phi(DatasetOperator):
    """Produces the Phi dataframe which contains every combination of users with rated films in common.

    This output will be on the order of N^2M and will be written to file. This class will create a DataFrame for each movie of the format:
    - movieId (int)
    - user (int): A user that rated movieId
    - neighbor (int): A user who also rated movieId

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

    Returns Dataset
    """

    def __init__(
        self,
        step_params: OperatorParams,
        input_params: OperatorParams,
        output_params: OperatorParams,
    ) -> None:
        super().__init__(step_params, input_params, output_params)

    def execute(self, data: Dataset = None, context: Context = None, *args, **kwargs) -> Dataset:

        data = self.input_dataset

        phi = data.merge(data, how="inner", on="movieId")
        # Remove rows where userId_x and userId_y are equal
        phi = phi.loc[phi["userId_x"] != phi["userId_y"]]

        params = self._output_params.as_dict()

        dataset = Dataset(**params, data=phi)

        return dataset


# ------------------------------------------------------------------------------------------------ #
class UserWeights(DatasetOperator):
    """Computes user-user pearson correlation coefficients representing similarity between users.

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
        name: str,
        input_params: Union[int, List[int]],
        output_params: dict,
        description: str = None,
    ) -> None:
        name = self.__class__.__name__.lower() if name is None else name
        super().__init__(
            name=name,
            input_params=input_params,
            output_params=output_params,
            description=description,
        )

    def execute(self, data: Dataset = None, context: Context = None, *args, **kwargs) -> Dataset:
        """Executes the operation

        Args:
            data (pd.DataFrame): The user neighbor rating data
        """

        data = self.input_data.data

        weights = self._compute_weights(data=data)

        dataset = Dataset(**self._output_dataset_params, data=weights)

        return dataset

    def _compute_weights(self, data: pd.DataFrame) -> pd.DataFrame:
        """Computes the pearson's correlation for each user and her neighbors

        Technically, the algorithm calls for pearson's correlation coefficient, which
        centers the data. Since the ratings have already been centered,
        the calculation below is equivalent to cosine similarity.

        Args:
            data (pd.DataFrame): The dataframe containing users and neighbors
        """

        weights = (
            data.groupby(["userId_x", "userId_y"])
            .progress_apply(
                lambda x: (
                    np.dot(x["adj_rating_x"], x["adj_rating_y"])
                    / (
                        np.sqrt(
                            np.sum(np.square(x["adj_rating_x"]))
                            * np.sum(np.square(x["adj_rating_y"]))
                        )
                    )
                )
            )
            .reset_index()
        )

        weights.columns = ["userId", "neighbor", "weight"]

        return weights


# ------------------------------------------------------------------------------------------------ #


class DataIntegrator(DatasetOperator):
    """Integrates user, rating, and weight data

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
        name: str,
        input_params: Union[int, List[int]],
        output_params: dict,
        description: str = None,
    ) -> None:
        name = self.__class__.__name__.lower() if name is None else name
        super().__init__(
            name=name,
            input_params=input_params,
            output_params=output_params,
            description=description,
        )
        self._users = None
        self._ratings = None
        self._weights = None

    def execute(self, data: Dataset = None, context: Context = None, *args, **kwargs) -> Dataset:
        """Executes the operation

        Args:
            data (pd.DataFrame): Not used.
        """
        self._load_data()

        # Add Users and Weights
        data = self._users.merge(self._weights, how="left", on="userId")
        # Merge in ratings for the neighbors into the dataframe
        data = data.merge(self._ratings, how="left", left_on="neighbor", right_on="userId")

        dataset = Dataset(**self._output_dataset_params, data=data)

        return dataset

    def _load_data(self) -> None:
        """Loads user, ratings, and weights data"""

        self._users = self.input_params["users"]
        self._ratings = self.input_params["ratings"]
        self._weights = self.input_params["weights"]


# ------------------------------------------------------------------------------------------------ #
