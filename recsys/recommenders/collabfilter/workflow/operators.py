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
# Created    : Friday November 25th 2022 11:49:36 pm                                               #
# Modified   : Saturday November 26th 2022 12:42:35 pm                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Collaborative Filtering Data Preprocessing Module."""
import numpy as np
import pandas as pd
import logging
from typing import Union, List

from recsys.config.base import OperatorParams
from recsys.core.workflow.pipeline import Context
from recsys.core.workflow.operators import DatasetOperator
from recsys.core.dal.dataset import Dataset

# ------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(__name__)
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

        result = {"train": train, "test": test}

        return result


# ------------------------------------------------------------------------------------------------ #
#                                      RATINGS ADJUSTER                                            #
# ------------------------------------------------------------------------------------------------ #


class DataCentralizer(DatasetOperator):
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

        params = self.output_params.as_dict()

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
        logger.debug(f"\n\nPhi merge complete with {phi.shape[0]} rows.")
        # Remove rows where userId_x and userId_y are equal
        phi = phi.loc[phi["userId_x"] != phi["userId_y"]]
        logger.debug(f"Phi dedup complete with {phi.shape[0]} rows.")

        params = self.output_params.as_dict()

        dataset = Dataset(**params, data=phi)
        logger.debug("Dataset instantiated.")

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
