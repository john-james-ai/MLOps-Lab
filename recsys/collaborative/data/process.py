#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /process.py                                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday November 17th 2022 06:37:35 am                                             #
# Modified   : Friday November 18th 2022 10:28:00 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Data Processing Module"""
import logging
from typing import Dict
import pandas as pd
import numpy as np
from numpy.random import default_rng
from tqdm import tqdm

from recsys.core.base.workflow import DatasetOperator, Pipeline

from recsys.core.dal.dataset import Dataset, DatasetParams
from recsys.core.dal.repo import repository

# ------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #
tqdm.pandas()  # Supports progress monitoring of pandas groupby operations using tqdm
# ------------------------------------------------------------------------------------------------ #


class TrainTestSplit(DatasetOperator):
    """Splits the dataset into to training and test sets by user

    Args:
        name (str): Name given to this instance of the operator.
        description (str): Description assigned to this instance of the operator
        input_dataset_params (str): A dataset parameter object or list of objects, defining the input.
        output_dataset_params (dict): Dictionary of parameter objects for the train and test datasets.
        clustered (bool): Whether to cluster sampling.
        clustered_by (str): The column by which to cluster.
        train_proportion (float): The proportion of the dataset to allocate to training.
        random_state (int): PseudoRandom generator seed
        force (bool): If True, overwrite existing TrainTest splits

    Returns: Dictionary of Dataset objects containing train and test Dataset objects.

    """

    def __init__(
        self,
        input_dataset_params: DatasetParams,
        output_dataset_params: Dict[DatasetParams],
        clustered_by: str = None,
        clustered: bool = False,
        train_proportion: float = 0.8,
        random_state: int = None,
        name: str = None,
        description: str = None,
        force: bool = False,
    ) -> None:
        super().__init__(
            input_dataset_params=input_dataset_params,
            output_dataset_params=output_dataset_params,
            clustered=clustered,
            clustered_by=clustered_by,
            train_proportion=train_proportion,
            random_state=random_state,
            name=name,
            description=description,
            force=force,
        )

    @repository
    def execute(self, *args, **kwargs) -> Dict[Dataset]:

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
        train = self._output_dataset_params["train"].to_dataset()
        train.data = train_set

        test = self._output_dataset_params["test"].to_dataset()
        test.data = test_set

        outputs = {"train": train, "test": test}

        return outputs


# ------------------------------------------------------------------------------------------------ #


class RatingsAdjuster(DatasetOperator):
    """Centers the ratings by subtracting the users average rating from each of the users ratings.

    Args:
        name (str): Name given to this instance of the operator.
        description (str): Description assigned to this instance of the operator
        input_dataset_params (str): A dataset parameter object or list of objects, defining the input.
        output_dataset_params (dict): Dictionary of parameter objects for the train and test datasets.
        force (bool): If True, overwrite existing TrainTest splits

    Returns Dataset
    """

    def __init__(
        self,
        input_dataset_params: DatasetParams,
        output_dataset_params: DatasetParams,
        name: str = None,
        description: str = None,
        force: bool = False,
    ) -> None:
        name = self.__class__.__name__.lower() if name is None else name
        super().__init__(
            name=name,
            description=description,
            input_dataset_params=input_dataset_params,
            output_dataset_params=output_dataset_params,
            force=force,
        )

    def execute(self, *args, **kwargs) -> Dataset:

        data = self.input_data.data

        data["adj_rating"] = data["rating"].sub(data.groupby("userId")["rating"].transform("mean"))

        dataset = self._output_dataset_params.to_dataset()
        dataset.data = data

        return dataset


class User(DatasetOperator):
    """Computes and stores average rating for each user.

    Args:
        name (str): Name given to this instance of the operator.
        description (str): Description assigned to this instance of the operator
        input_dataset_params (str): A dataset parameter object or list of objects, defining the input.
        output_dataset_params (dict): Dictionary of parameter objects for the train and test datasets.
        force (bool): If True, overwrite existing TrainTest splits

    Returns Dataset
    """

    def __init__(
        self,
        input_dataset_params: DatasetParams,
        output_dataset_params: DatasetParams,
        name: str = None,
        description: str = None,
        force: bool = False,
    ) -> None:
        name = self.__class__.__name__.lower() if name is None else name
        super().__init__(
            name=name,
            description=description,
            input_dataset_params=input_dataset_params,
            output_dataset_params=output_dataset_params,
            force=force,
        )

    def execute(self, *args, **kwargs) -> Dataset:

        data = self.input_data.data

        user_average_ratings = data.groupby("userId")["rating"].mean().reset_index()
        user_average_ratings.columns = ["userId", "mean_rating"]

        dataset = self._output_dataset_params.to_dataset()
        dataset.data = user_average_ratings

        return dataset


# ------------------------------------------------------------------------------------------------ #
class Phi(DatasetOperator):
    """Produces the Phi dataframe which contains every combination of users with rated films in common.

    This output will be on the order of N^2M and will be written to file. This class will create a DataFrame for each movie of the format:
    - movieId (int)
    - user (int): A user that rated movieId
    - neighbor (int): A user who also rated movieId

    Args:
        name (str): Name given to this instance of the operator.
        description (str): Description assigned to this instance of the operator
        input_dataset_params (str): A dataset parameter object or list of objects, defining the input.
        output_dataset_params (dict): Dictionary of parameter objects for the train and test datasets.
        force (bool): If True, overwrite existing TrainTest splits

    Returns Dataset
    """

    def __init__(
        self,
        input_dataset_params: DatasetParams,
        output_dataset_params: DatasetParams,
        name: str = None,
        description: str = None,
        force: bool = False,
    ) -> None:
        name = self.__class__.__name__.lower() if name is None else name
        super().__init__(
            name=name,
            description=description,
            input_dataset_params=input_dataset_params,
            output_dataset_params=output_dataset_params,
            force=force,
        )

    def execute(self, *args, **kwargs) -> Dataset:

        data = self.input_data.data

        phi = data.merge(data, how="inner", on="movieId")
        # Remove rows where userId_x and userId_y are equal
        phi = phi.loc[phi["userId_x"] != phi["userId_y"]]

        dataset = self._output_dataset_params.to_dataset()
        dataset.data = phi

        return dataset


# ------------------------------------------------------------------------------------------------ #
class UserWeights(DatasetOperator):
    """Computes user-user pearson correlation coefficients representing similarity between users.

    Args:
        name (str): Name given to this instance of the operator.
        description (str): Description assigned to this instance of the operator
        input_dataset_params (str): A dataset parameter object or list of objects, defining the input.
        output_dataset_params (dict): Dictionary of parameter objects for the train and test datasets.
        force (bool): If True, overwrite existing TrainTest splits

    Returns Dataset
    """

    def __init__(
        self,
        input_dataset_params: DatasetParams,
        output_dataset_params: DatasetParams,
        name: str = None,
        description: str = None,
        force: bool = False,
    ) -> None:
        name = self.__class__.__name__.lower() if name is None else name
        super().__init__(
            name=name,
            description=description,
            input_dataset_params=input_dataset_params,
            output_dataset_params=output_dataset_params,
            force=force,
        )

    def execute(self, *args, **kwargs) -> Dataset:
        """Executes the operation

        Args:
            data (pd.DataFrame): The user neighbor rating data
        """

        data = self.input_data.data

        weights = self._compute_weights(data=data)

        dataset = self._output_dataset_params.to_dataset()
        dataset.data = weights

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
        name (str): Name given to this instance of the operator.
        description (str): Description assigned to this instance of the operator
        input_dataset_params (str): A dataset parameter object or list of objects, defining the input.
        output_dataset_params (dict): Dictionary of parameter objects for the train and test datasets.
        force (bool): If True, overwrite existing TrainTest splits

    Returns Dataset
    """

    def __init__(
        self,
        input_dataset_params: DatasetParams,
        output_dataset_params: DatasetParams,
        name: str = None,
        description: str = None,
        force: bool = False,
    ) -> None:
        name = self.__class__.__name__.lower() if name is None else name
        super().__init__(
            name=name,
            description=description,
            input_dataset_params=input_dataset_params,
            output_dataset_params=output_dataset_params,
            force=force,
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

        dataset = self._output_dataset_params.to_dataset()
        dataset.data = data

        return dataset

    def _load_data(self) -> None:
        """Loads user, ratings, and weights data"""

        self._users = self.input_data["users"]
        self._ratings = self.input_data["ratings"]
        self._weights = self.input_data["weights"]

    def _save_data(self, dataset: pd.DataFrame) -> None:
        """Saves the predictions to file."""
        self._io.write(self.output_filepath, dataset)


# ------------------------------------------------------------------------------------------------ #


class ETLPipeline(Pipeline):
    """ETL Pipeline class

    Executes Extract-Transform-Load pipelines

    Args:
        name (str): Human readable name for the pipeline run.
        description (str): Optional.
    """

    def __init__(self, name: str, description: str = None) -> None:
        super().__init__(name=name, description=description)

    def run(self) -> None:
        """Runs the pipeline"""
        self._setup()
        for name, step in tqdm(self._steps.items()):
            started = datetime.now()

            step.execute(context=self._context)

            ended = datetime.now()
            duration = (ended - started).total_seconds()
            wandb.log(
                {
                    "pipeline": self._context.name,
                    "step": name,
                    "started": started,
                    "ended": ended,
                    "duration": duration,
                }
            )
        self._teardown()


# ------------------------------------------------------------------------------------------------ #
class PreprocessPipelineBuilder(PipelineBuilder):
    """Constructs an Preprocess processing pipeline."""

    def __init__(self) -> None:
        self.reset()
        self._config = None
        self._context = None
        self._steps = {}

    def build_config(self, config: dict) -> None:
        self._config = config

    def build_context(self) -> None:
        try:
            module = importlib.import_module(name=self._config["io_module"])
            io = getattr(module, self._config["io_service"])
            self._context = Context(
                name=self._config["name"], description=self._config["description"], io=io
            )
        except KeyError as e:
            logger.error(e)
            raise

    def build_steps(self) -> None:
        steps = self._config["steps"]
        for _, step_config in steps.items():
            try:
                module = importlib.import_module(name=step_config["module"])
                step = getattr(module, step_config["operator"])

                operator = step(**step_config["params"])

                self._steps[operator.name] = operator

            except KeyError as e:
                logging.error("Configuration File is missing operator configuration data")
                raise (e)

    def build_pipeline(self) -> None:
        logger.debug(
            f"Config name: {self._config['name']}, Config description: {self._config['description']}"
        )
        self._pipeline = PreprocessPipeline(
            name=self._config["name"], description=self._config["description"]
        )
        logger.debug(self._pipeline)
        self._pipeline.context = self._context
        for _, step in self._steps.items():
            self._pipeline.add_step(step)


# ------------------------------------------------------------------------------------------------ #
class PreprocessPipelineDirector(PipelineDirector):
    """Preprocess Pipelne director responsible for executing the steps of the PipelineBuilder in a sequence.

    Args:
        config_filepath (str): The path to the pipeline configuration file
        builder (PreprocessPipelineBuilder): The concrete builder class
    """

    def __init__(self, config_filepath: str, builder: PreprocessPipelineBuilder) -> None:
        super().__init__(config_filepath=config_filepath, builder=builder)

    def build_etl_pipeline(self) -> None:
        """Constructs the Preprocess Pipeline"""
        self._builder.build_config(config=self._config)
        self._builder.build_context()
        self._builder.build_steps()
        self._builder.build_pipeline()
