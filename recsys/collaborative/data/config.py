#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /config.py                                                                          #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday November 10th 2022 06:53:48 pm                                             #
# Modified   : Thursday November 17th 2022 07:04:32 am                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
from dataclasses import dataclass, field
from recsys.core.services.io import IOService
from recsys.core.base.config import Config

# ------------------------------------------------------------------------------------------------ #
#                                        ETL CONFIG                                                #
# ------------------------------------------------------------------------------------------------ #


@dataclass
class DownloadConfig(Config):
    kaggle_filepath: str = "grouplens/movielens-20m-dataset"
    destination: str = "data/movielens20m/ext"


# ------------------------------------------------------------------------------------------------ #


@dataclass
class DeZipConfig(Config):
    zipfilepath: str = "02-Collaborative-Filtering/tests/data/ext/movielens-20m-dataset.zip"
    destination: str = "02-Collaborative-Filtering/tests/data/raw"
    members: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        self.members = ["rating.csv"]


# ------------------------------------------------------------------------------------------------ #


@dataclass
class PicklerConfig(Config):
    infilepath: str = "02-Collaborative-Filtering/tests/data/raw/rating.csv"
    outfilepath: str = "02-Collaborative-Filtering/tests/data/raw/rating.pkl"
    infile_format: str = "csv"
    usecols: list[str] = field(default_factory=list)
    index_col: bool = False
    encoding: str = "utf-8"
    low_memory: bool = False

    def __post_init__(self) -> None:
        self.usecols = ["userId", "movieId", "rating"]


# ------------------------------------------------------------------------------------------------ #
#                                       DATASET CONFIG                                             #
# ------------------------------------------------------------------------------------------------ #


@dataclass
class DatasetConfig(Config):
    filepath: str = "02-Collaborative-Filtering/tests/data/raw/rating.pkl"
    fileformat: str = "pkl"
    io_service: IOService = IOService()


# ------------------------------------------------------------------------------------------------ #
#                                       PREPROCESS CONFIG                                          #
# ------------------------------------------------------------------------------------------------ #


@dataclass
class TrainTestSplitConfig(Config):
    output: bool = True
    output_filepath: str = "02-Collaborative-Filtering/tests/data/train_test"
    output_format: str = "pkl"
    IOService: IOService = IOService()
    clustered: bool = True
    clustered_by: str = "userId"
    train_proportion: float = 0.8
    random_state: int = 123


# ------------------------------------------------------------------------------------------------ #


@dataclass
class UserConfig(Config):
    output: bool = True
    output_filepath: str = "02-Collaborative-Filtering/tests/data/preprocess/users.pkl"
    output_format: str = "pkl"
    IOService: IOService = IOService()


# ------------------------------------------------------------------------------------------------ #


@dataclass
class RatingsAdjusterConfig(Config):
    output: bool = True
    output_filepath: str = "02-Collaborative-Filtering/tests/data/preprocess/ratings_adj.pkl"
    output_format: str = "pkl"
    IOService: IOService = IOService()


# ------------------------------------------------------------------------------------------------ #


@dataclass
class PhiConfig(Config):
    output: bool = True
    output_filepath: str = "02-Collaborative-Filtering/tests/data/preprocess/phi.pkl"
    output_format: str = "pkl"
    IOService: IOService = IOService()


# ------------------------------------------------------------------------------------------------ #


@dataclass
class UserWeightsConfig(Config):
    output: bool = True
    output_filepath: str = "02-Collaborative-Filtering/tests/data/preprocess/weights.pkl"
    output_format: str = "pkl"
    IOService: IOService = IOService()


# ------------------------------------------------------------------------------------------------ #


@dataclass
class DataIntegratorConfig(Config):
    users_filepath: str = "02-Collaborative-Filtering/tests/data/preprocess/users.pkl"
    ratings_filepath: str = "02-Collaborative-Filtering/tests/data/preprocess/ratings_adj.pkl"
    weights_filepath: str = "02-Collaborative-Filtering/tests/data/preprocess/weights.pkl"
    output: bool = True
    output_filepath: str = "02-Collaborative-Filtering/tests/data/preprocess/dataset.pkl"
    output_format: str = "pkl"
    IOService: IOService = IOService()
