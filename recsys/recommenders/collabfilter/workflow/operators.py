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
# Created    : Sunday November 27th 2022 06:59:08 am                                               #
# Modified   : Wednesday November 30th 2022 12:55:08 am                                            #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
#                     COLLABORATIVE FILTERING DATA PREP OPERATORS                                  #
# ================================================================================================ #
import os
from dataclasses import dataclass
from dotenv import load_dotenv
import numpy as np
import logging
from copy import deepcopy

from recsys.config.data import DIRECTORIES, TRAIN_PROPORTION
from recsys.config.workflow import RANDOM_STATE
from recsys.core.workflow.operators import DatasetOperator
from recsys.core.workflow.pipeline import Context
from recsys.core.dal.dataset import Dataset
from recsys.config.workflow import (
    StepPO,
    InputPO,
    OutputPO,
    FilesetInputPO,
    DatasetInputPO,
    DatasetOutputPO,
    DatasetGroupPO,
    DatasetGroupABC,
)

# ------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ #
#                                      CREATE DATASET                                              #
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ #
INPUT_DIRECTORIES = {
    "prod": os.path.join(DIRECTORIES["data"]["prod"], "input", "rating.pkl"),
    "dev": os.path.join(DIRECTORIES["data"]["dev"], "input", "rating.pkl"),
    "test": os.path.join(DIRECTORIES["data"]["test"], "input", "rating.pkl"),
}
# ------------------------------------------------------------------------------------------------ #
#                                 PARAMETER CONFIGURATIONS                                         #
# ------------------------------------------------------------------------------------------------ #


@dataclass
class CreateDatasetStepPO(StepPO):
    name: str = "create_rating_dataset"
    description: str = "Creates rating dataset"
    force: bool = False


# ------------------------------------------------------------------------------------------------ #


@dataclass
class CreateDatasetInputPO(FilesetInputPO):
    filepath: str = None

    def __post_init__(self) -> None:
        load_dotenv()
        ENV = os.getenv("ENV")
        self.filepath = INPUT_DIRECTORIES[ENV]


# ------------------------------------------------------------------------------------------------ #
@dataclass
class CreateDatasetOutputPO(DatasetOutputPO):
    name: str = "rating"
    description: str = "Rating Dataset"
    stage: str = "staged"


# ------------------------------------------------------------------------------------------------ #
#                          CREATE DATASET OPERATOR                                                 #
# ------------------------------------------------------------------------------------------------ #


class CreateDataset(DatasetOperator):
    """Reads a DataFrame, creates a Dataset and commits it to the repository.

    Args:
        step_params (StepPO): Parameters which control operator identity and behavior.
        input_params (InputPO): Input filepath
        output_params (OutputPO). Parameters for the Dataset to be created.

    Returns: Output Dataset Object
    """

    def __init__(
        self,
        step_params: StepPO = CreateDatasetStepPO(),
        input_params: InputPO = CreateDatasetInputPO(),
        output_params: OutputPO = CreateDatasetOutputPO(),
    ) -> None:
        super().__init__(step_params, input_params, output_params)

    def execute(self, data: Dataset = None, context: Context = None, *args, **kwargs) -> Dataset:
        """Creates the dataset."""

        io = context.io
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
            stage=self.output_params.stage,
            data=data,
        )

        return dataset


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ #
#                                    TRAIN TEST SPLIT                                              #
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ #
# ------------------------------------------------------------------------------------------------ #
#                            TRAIN TEST SPLIT PARAMETER CONFIG                                     #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class TrainTestStepPO(StepPO):
    name: str = "train_test_split"
    description: str = "User Clustered Train-Test Split"
    train_proportion: float = TRAIN_PROPORTION
    clustered: bool = True
    clustered_by: str = "userId"
    random_state: int = RANDOM_STATE
    force: bool = True


# ------------------------------------------------------------------------------------------------ #
@dataclass
class TrainTestInputPO(DatasetInputPO):
    name: str = "rating"
    stage: str = "staged"


# ------------------------------------------------------------------------------------------------ #
@dataclass
class TrainSetOutputPO(DatasetOutputPO):
    name: str = "train"
    description: str = "Train Set"
    stage: str = "split"


# ------------------------------------------------------------------------------------------------ #
@dataclass
class TestSetOutputPO(DatasetOutputPO):
    name: str = "test"
    description: str = "Test Set"
    stage: str = "split"


# ------------------------------------------------------------------------------------------------ #
@dataclass
class TrainTestOutputPO(DatasetGroupPO):
    train: TrainSetOutputPO = TrainSetOutputPO()
    test: TestSetOutputPO = TestSetOutputPO()

    def get_datasets(self) -> dict:
        d = {"train": self.train, "test": self.test}
        return d


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DatasetGroup(DatasetGroupABC):
    train: Dataset
    test: Dataset

    def get_datasets(self) -> dict:
        d = {"train": self.train, "test": self.test}
        return d


# ------------------------------------------------------------------------------------------------ #
#                               TRAIN TEST SPLIT OPERATOR                                          #
# ------------------------------------------------------------------------------------------------ #
class TrainTestSplit(DatasetOperator):
    """Splits the dataset into to training and test sets by user

    Args:
        step_params (StepPO): Training proportion and random state parameters.
        input_params (InputPO): Input Dataset name and stage.
        output_params (TrainTestOutput) Dictionary of Train and Test set Datasets.

    Returns: Dictionary of Train and Test Dataset objects.

    """

    def __init__(
        self,
        step_params: StepPO = TrainTestStepPO(),
        input_params: InputPO = TrainTestInputPO(),
        output_params: OutputPO = TrainTestOutputPO(),
    ) -> None:
        super().__init__(step_params, input_params, output_params)

    def execute(self, data: Dataset = None, context: Context = None, *args, **kwargs) -> Dataset:

        data = self.input_data

        if self.clustered:

            rng = np.random.default_rng(self.random_state)

            clusters = data[self.clustered_by].unique()
            train_set_size = int(len(clusters) * self.train_proportion)

            train_clusters = rng.choice(
                a=clusters, size=train_set_size, replace=False, shuffle=True
            )
            test_clusters = np.setdiff1d(clusters, train_clusters)

            train_set = data.loc[data[self.clustered_by].isin(train_clusters)]
            test_set = data.loc[data[self.clustered_by].isin(test_clusters)]

        else:
            # Get all indices
            index = np.array(data.index.to_numpy())

            # Split the training set by the train proportion
            train_set = data.sample(
                frac=self.train_proportion,
                replace=False,
                axis=0,
                random_state=self.random_state,
            )

            # Obtain training indices and perform setdiff to get test indices
            train_idx = train_set.index
            test_idx = np.setdiff1d(index, train_idx)

            # Extract test data
            test_set = data.loc[test_idx]

        # Create train and test Dataset objects.
        train_params = self.output_params.train.as_dict()
        test_params = self.output_params.test.as_dict()

        train = deepcopy(Dataset(**train_params, data=train_set))
        test = deepcopy(Dataset(**test_params, data=test_set))

        result = DatasetGroup(train=train, test=test)

        return result


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ #
#                                    DATA CENTRALIZER                                              #
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ #

# ------------------------------------------------------------------------------------------------ #
#                           DATA CENTRALIZER PARAMETER CONFIG                                      #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class TrainDataCentralizerStepPO(StepPO):
    name: str = "train_data_centralizer"
    description: str = "Center Ratings by User Mean"
    force: bool = True


# ------------------------------------------------------------------------------------------------ #
@dataclass
class TrainDataCentralizerInputPO(DatasetInputPO):
    name: str = "train"
    stage: str = "split"


# ------------------------------------------------------------------------------------------------ #
@dataclass
class TrainDataCentralizerOutputPO(DatasetOutputPO):
    name: str = "train_ratings_centered"
    description: str = "Training User Ratings Centered by User Mean Rating"
    stage: str = "interim"


# ------------------------------------------------------------------------------------------------ #
@dataclass
class TestDataCentralizerStepPO(StepPO):
    name: str = "test_data_centralizer"
    description: str = "Center Ratings by User Mean"
    force: bool = True


# ------------------------------------------------------------------------------------------------ #
@dataclass
class TestDataCentralizerInputPO(DatasetInputPO):
    name: str = "test"
    stage: str = "split"


# ------------------------------------------------------------------------------------------------ #
@dataclass
class TestDataCentralizerOutputPO(DatasetOutputPO):
    name: str = "test_ratings_centered"
    description: str = "Test User Ratings Centered by User Mean Rating"
    stage: str = "interim"


# ------------------------------------------------------------------------------------------------ #
#                               DATA CENTRALIZER OPERATOR                                          #
# ------------------------------------------------------------------------------------------------ #


class TrainDataCentralizer(DatasetOperator):
    """Centers the ratings by subtracting the users average rating from each of the users ratings.

    Args:
        step_params (StepPO): Name and description of the step
        input_params (InputPO): The train Dataset params
        output_params (DatasetOutputPO) Train centralized ratings

    Returns: Output Dataset Object
    """

    def __init__(
        self,
        step_params: StepPO = TrainDataCentralizerStepPO(),
        input_params: DatasetInputPO = TrainDataCentralizerInputPO(),
        output_params: DatasetOutputPO = TrainDataCentralizerOutputPO(),
    ) -> None:
        super().__init__(step_params, input_params, output_params)

    def execute(self, data: Dataset = None, context: Context = None, *args, **kwargs) -> Dataset:

        data = self.input_data

        data["rating_centered"] = data["rating"].sub(
            data.groupby("userId")["rating"].transform("mean")
        )

        params = self.output_params.as_dict()

        dataset = Dataset(**params, data=data)

        return dataset


# ------------------------------------------------------------------------------------------------ #


class TestDataCentralizer(DatasetOperator):
    """Centers the ratings by subtracting the users average rating from each of the users ratings.

    Args:
        step_params (StepPO): Name and description of the step
        input_params (InputPO): The test Dataset params
        output_params (DatasetOutputPO) Test centralized ratings

    Returns: Output Dataset Object
    """

    def __init__(
        self,
        step_params: StepPO = TestDataCentralizerStepPO(),
        input_params: DatasetInputPO = TestDataCentralizerInputPO(),
        output_params: DatasetOutputPO = TestDataCentralizerOutputPO(),
    ) -> None:
        super().__init__(step_params, input_params, output_params)

    def execute(self, data: Dataset = None, context: Context = None, *args, **kwargs) -> Dataset:

        data = self.input_data

        data["rating_centered"] = data["rating"].sub(
            data.groupby("userId")["rating"].transform("mean")
        )

        params = self.output_params.as_dict()

        dataset = Dataset(**params, data=data)

        return dataset


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ #
#                                   USER AVERAGE SCORES                                            #
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ #


# ------------------------------------------------------------------------------------------------ #
#                                 USER PARAMETER CONFIG                                            #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class TrainUserStepPO(StepPO):
    name: str = "train_user"
    description: str = "User Average Ratings"
    force: bool = True


# ------------------------------------------------------------------------------------------------ #


@dataclass
class TrainUserInputPO(DatasetInputPO):
    name: str = "train"
    stage: str = "split"


# ------------------------------------------------------------------------------------------------ #
@dataclass
class TrainUserOutputPO(DatasetOutputPO):
    name: str = "train_user_ave_ratings"
    description: str = "Train User Average Ratings"
    stage: str = "interim"


# ------------------------------------------------------------------------------------------------ #
@dataclass
class TestUserStepPO(StepPO):
    name: str = "test_user"
    description: str = "User Average Ratings"
    force: bool = True


# ------------------------------------------------------------------------------------------------ #


@dataclass
class TestUserInputPO(DatasetInputPO):
    name: str = "test"
    stage: str = "split"


# ------------------------------------------------------------------------------------------------ #
@dataclass
class TestUserOutputPO(DatasetOutputPO):
    name: str = "test_user_ave_ratings"
    description: str = "Test User Average Ratings"
    stage: str = "interim"


# ------------------------------------------------------------------------------------------------ #
#                                     USER OPERATOR                                                #
# ------------------------------------------------------------------------------------------------ #


class TrainUser(DatasetOperator):
    """Computes and stores average rating for each user.

    Args:
        step_params (StepPO): Name and description of the step
        input_params (InputPO): The train user Dataset params
        output_params (Dict[str, DatasetOutputPO]) Train user average ratings

    Returns: Output Dataset Object

    Returns Dataset
    """

    def __init__(
        self,
        step_params: StepPO = TrainUserStepPO(),
        input_params: DatasetInputPO = TrainUserInputPO(),
        output_params: DatasetOutputPO = TrainUserOutputPO(),
    ) -> None:
        super().__init__(step_params, input_params, output_params)

    def execute(self, data: Dataset = None, context: Context = None, *args, **kwargs) -> Dataset:

        data = self.input_data

        user_average_ratings = data.groupby("userId")["rating"].mean().reset_index()
        user_average_ratings.columns = ["userId", "mean_rating"]

        params = self.output_params.as_dict()

        dataset = Dataset(**params, data=user_average_ratings)

        return dataset


# ------------------------------------------------------------------------------------------------ #


class TestUser(DatasetOperator):
    """Computes and stores average rating for each user.

    Args:
        step_params (StepPO): Name and description of the step
        input_params (DatasetInputPO): The test user Dataset params
        output_params (DatasetOutputPO) Test user average ratings

    Returns: Output Dataset Object

    Returns Dataset
    """

    def __init__(
        self,
        step_params: StepPO = TestUserStepPO(),
        input_params: DatasetInputPO = TestUserInputPO(),
        output_params: DatasetOutputPO = TestUserOutputPO(),
    ) -> None:
        super().__init__(step_params, input_params, output_params)

    def execute(self, data: Dataset = None, context: Context = None, *args, **kwargs) -> Dataset:

        data = self.input_data

        user_average_ratings = data.groupby("userId")["rating"].mean().reset_index()
        user_average_ratings.columns = ["userId", "mean_rating"]

        params = self.output_params.as_dict()

        dataset = Dataset(**params, data=user_average_ratings)

        return dataset
