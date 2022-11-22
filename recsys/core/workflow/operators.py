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
# Modified   : Monday November 21st 2022 08:44:03 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import os
from typing import Any, Union, Dict
import numpy as np
from numpy.random import default_rng
import pandas as pd
from abc import ABC, abstractmethod
import logging
import shlex
import subprocess
from zipfile import ZipFile

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

    def __init__(self, **kwargs) -> None:
        self.name = None
        for k, v in kwargs.items():
            setattr(self, k, v)

        self.name = self.__class__.__name__.lower() if not self.name else self.name

    def __str__(self) -> str:
        return f"{self.__class__.__name__}\n\tAttributes: {self.__dict__.items()}"

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}\n\tAttributes: {self.__dict__.items()}"

    @profiler
    @abstractmethod
    def run(self, data: pd.DataFrame = None, context: Context = None, **kwargs) -> Any:
        pass


# ------------------------------------------------------------------------------------------------ #
class DatasetOperator(ABC):
    """Base class for operators that interact with Dataset objects in the Dataset Repository.

    Args:
        name (str): The name for the operator that distinguishes it in the pipeline.
        description (str): A description for the operator instance. Optional
        dataset_in_params (Union[int, List[int]]): Int or list of ints representing Dataset ids.
        dataset_out_params (dict): Parameters for output Dataset object.

    Attributes:
        input_dataset (Union[Dataset, List[Dataset]). The input Dataset or Datasets is/are
            injected by the repository decorator.

    Returns: Output Dataset Object
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(kwargs)

        self._input_dataset = None

    @property
    def input_dataset(self) -> Union[Dataset, Dict[str, Dataset]]:
        return self._input_dataset

    @input_dataset.setter
    def input_dataset(self, input_dataset: Union[Dataset, Dict[str, Dataset]]) -> None:
        self._input_dataset = input_dataset

    @profiler
    @repository
    @abstractmethod
    def execute(
        self, context: Context = None, *args, **kwargs
    ) -> Union[Dataset, Dict[str, Dataset]]:
        pass


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


# ------------------------------------------------------------------------------------------------ #
#                                     CREATE DATASET                                               #
# ------------------------------------------------------------------------------------------------ #


class CreateDataset(DatasetOperator):
    """Reads a DataFrame, creates a Dataset and commits it to the repository.

    Args:
        name (str): The name for the operator that distinguishes it in the pipeline.
        description (str): A description for the operator instance. Optional
        filepath (str): The filepath to the input object.
        dataset_in_params (Union[int, List[int]]): Not used by this Operator
        dataset_out_params (dict): Parameters for output Dataset object.

    Attributes:
        input_dataset (Union[Dataset, List[Dataset]). The input Dataset or Datasets is/are
            injected by the repository decorator.

    Returns: Output Dataset Object
    """

    def __init__(
        self, name: str, infilepath: str, dataset_out_params: dict, description: str = None
    ) -> None:
        super().__init__(
            self,
            name=name,
            description=description,
            infilepath=infilepath,
            dataset_out_params=dataset_out_params,
        )

    @repository
    def execute(self, context: Context = None, *args, **kwargs) -> Dataset:
        """Creates the dataset."""
        io = context.io
        data = io.read(self.infilepath)
        dataset = Dataset(
            name=self.dataset_out_params["name"],
            description=self.dataset_out_params["description"],
            stage=self.dataset_out_params["stage"],
            env=self.dataset_out_params["env"],
            data=data,
        )
        return dataset


# ------------------------------------------------------------------------------------------------ #
#                                   TRAIN TEST SPLIT                                               #
# ------------------------------------------------------------------------------------------------ #
class TrainTestSplit(DatasetOperator):
    """Splits the dataset into to training and test sets by user

    Args:
        name (str): The name for the operator that distinguishes it in the pipeline.
        description (str): A description for the operator instance. Optional
        dataset_in_params (Union[int, List[int]]): Int or list of ints representing Dataset ids.
        dataset_out_params (dict): Parameters for output Dataset object.
        clustered (bool): Whether to cluster sampling.
        clustered_by (str): The column by which to cluster.
        train_proportion (float): The proportion of the dataset to allocate to training.
        random_state (int): PseudoRandom generator seed

    Attributes:
        input_dataset (Union[Dataset, List[Dataset]). The input Dataset or Datasets is/are
            injected by the repository decorator.

    Returns: Dictionary of Dataset objects containing train and test Dataset objects.

    """

    def __init__(
        self,
        dataset_in_params: dict,
        dataset_out_params: dict,
        clustered_by: str = None,
        clustered: bool = False,
        train_proportion: float = 0.8,
        random_state: int = None,
        name: str = None,
        description: str = None,
    ) -> None:
        super().__init__(
            dataset_in_params=dataset_in_params,
            dataset_out_params=dataset_out_params,
            clustered=clustered,
            clustered_by=clustered_by,
            train_proportion=train_proportion,
            random_state=random_state,
            name=name,
            description=description,
        )

    @repository
    def execute(self, *args, **kwargs) -> Dict[str, Dataset]:

        data = self.input_data.data

        if self.clustered:

            clusters = data[self.clustered_by].unique()
            train_set_size = int(len(clusters) * self.train_proportion)

            train_clusters = default_rng.choice(
                a=clusters, size=train_set_size, replace=False, shuffle=True
            )
            test_clusters = np.setdiff1d(clusters, train_clusters)

            train_set = data.loc[data[self.clustered_by].isin(train_clusters)]
            test_set = data.loc[data[self.clustered_by].isin(test_clusters)]

        else:
            # Get all indices
            index = np.array(data.index.to_numpy())

            # Split the training set by the train proportion
            train_set = data.sample(frac=self.train_proportion, replace=False, axis=0)

            # Obtain training indices and perform setdiff to get test indices
            train_idx = train_set.index
            test_idx = np.setdiff1d(index, train_idx)

            # Extract test data
            test_set = data.loc[test_idx]

        # Create train and test Dataset objects.
        train = Dataset(**self._output_dataset_params["train"], data=train_set)
        test = Dataset(**self._output_dataset_params["test"], data=test_set)

        outputs = {"train": train, "test": test}

        return outputs


# ------------------------------------------------------------------------------------------------ #


class RatingsAdjuster(DatasetOperator):
    """Centers the ratings by subtracting the users average rating from each of the users ratings.

    Args:
        name (str): The name for the operator that distinguishes it in the pipeline.
        description (str): A description for the operator instance. Optional
        dataset_in_params (Union[int, List[int]]): Int or list of ints representing Dataset ids.
        dataset_out_params (dict): Parameters for output Dataset object.

    Attributes:
        input_dataset (Union[Dataset, List[Dataset]). The input Dataset or Datasets is/are
            injected by the repository decorator.

    Returns: Output Dataset Object
    """

    def __init__(
        self,
        dataset_in: int,
        dataset_out: dict,
        name: str = None,
        description: str = None,
    ) -> None:
        name = self.__class__.__name__.lower() if name is None else name
        super().__init__(
            name=name, description=description, dataset_in=dataset_in, dataset_out=dataset_out
        )

    def execute(self, *args, **kwargs) -> Dataset:

        data = self.input_data.data

        data["adj_rating"] = data["rating"].sub(data.groupby("userId")["rating"].transform("mean"))

        dataset = Dataset(**self._output_dataset_params, data=data)

        return dataset


class User(DatasetOperator):
    """Computes and stores average rating for each user.

    Args:
        name (str): The name for the operator that distinguishes it in the pipeline.
        description (str): A description for the operator instance. Optional
        dataset_in_params (Union[int, List[int]]): Int or list of ints representing Dataset ids.
        dataset_out_params (dict): Parameters for output Dataset object.

    Attributes:
        input_dataset (Union[Dataset, List[Dataset]). The input Dataset or Datasets is/are
            injected by the repository decorator.

    Returns: Output Dataset Object

    Returns Dataset
    """

    def __init__(
        self,
        dataset_in: int,
        dataset_out: dict,
        name: str = None,
        description: str = None,
    ) -> None:
        name = self.__class__.__name__.lower() if name is None else name
        super().__init__(
            name=name, description=description, dataset_in=dataset_in, dataset_out=dataset_out
        )

    def execute(self, *args, **kwargs) -> Dataset:

        data = self.input_data.data

        user_average_ratings = data.groupby("userId")["rating"].mean().reset_index()
        user_average_ratings.columns = ["userId", "mean_rating"]

        dataset = Dataset(**self._output_dataset_params, data=user_average_ratings)

        return dataset


# ------------------------------------------------------------------------------------------------ #
class Phi(DatasetOperator):
    """Produces the Phi dataframe which contains every combination of users with rated films in common.

    This output will be on the order of N^2M and will be written to file. This class will create a DataFrame for each movie of the format:
    - movieId (int)
    - user (int): A user that rated movieId
    - neighbor (int): A user who also rated movieId

    Args:
        name (str): The name for the operator that distinguishes it in the pipeline.
        description (str): A description for the operator instance. Optional
        dataset_in_params (Union[int, List[int]]): Int or list of ints representing Dataset ids.
        dataset_out_params (dict): Parameters for output Dataset object.

    Attributes:
        input_dataset (Union[Dataset, List[Dataset]). The input Dataset or Datasets is/are
            injected by the repository decorator.

    Returns: Output Dataset Object

    Returns Dataset
    """

    def __init__(
        self,
        dataset_in: int,
        dataset_out: dict,
        name: str = None,
        description: str = None,
    ) -> None:
        name = self.__class__.__name__.lower() if name is None else name
        super().__init__(
            name=name, description=description, dataset_in=dataset_in, dataset_out=dataset_out
        )

    def execute(self, *args, **kwargs) -> Dataset:

        data = self.input_data.data

        phi = data.merge(data, how="inner", on="movieId")
        # Remove rows where userId_x and userId_y are equal
        phi = phi.loc[phi["userId_x"] != phi["userId_y"]]

        dataset = Dataset(**self._output_dataset_params, data=phi)

        return dataset


# ------------------------------------------------------------------------------------------------ #
class UserWeights(DatasetOperator):
    """Computes user-user pearson correlation coefficients representing similarity between users.

    Args:
        name (str): The name for the operator that distinguishes it in the pipeline.
        description (str): A description for the operator instance. Optional
        dataset_in_params (Union[int, List[int]]): Int or list of ints representing Dataset ids.
        dataset_out_params (dict): Parameters for output Dataset object.

    Attributes:
        input_dataset (Union[Dataset, List[Dataset]). The input Dataset or Datasets is/are
            injected by the repository decorator.

    Returns: Output Dataset Object
    """

    def __init__(
        self,
        dataset_in: int,
        dataset_out: dict,
        name: str = None,
        description: str = None,
    ) -> None:
        name = self.__class__.__name__.lower() if name is None else name
        super().__init__(
            name=name, description=description, dataset_in=dataset_in, dataset_out=dataset_out
        )

    def execute(self, *args, **kwargs) -> Dataset:
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
        name (str): The name for the operator that distinguishes it in the pipeline.
        description (str): A description for the operator instance. Optional
        dataset_in_params (Union[int, List[int]]): Int or list of ints representing Dataset ids.
        dataset_out_params (dict): Parameters for output Dataset object.

    Attributes:
        input_dataset (Union[Dataset, List[Dataset]). The input Dataset or Datasets is/are
            injected by the repository decorator.

    Returns: Output Dataset Object
    """

    def __init__(
        self,
        dataset_in: list,
        dataset_out: dict,
        name: str = None,
        description: str = None,
    ) -> None:
        name = self.__class__.__name__.lower() if name is None else name
        super().__init__(
            name=name, description=description, dataset_in=dataset_in, dataset_out=dataset_out
        )
        self._users = None
        self._ratings = None
        self._weights = None

    def execute(self, **kwargs) -> None:
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

        self._users = self.input_data["users"]
        self._ratings = self.input_data["ratings"]
        self._weights = self.input_data["weights"]


# ------------------------------------------------------------------------------------------------ #
