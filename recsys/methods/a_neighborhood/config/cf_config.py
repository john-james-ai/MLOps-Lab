#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /cf_config.py                                                                       #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday December 1st 2022 12:25:04 am                                              #
# Modified   : Thursday December 1st 2022 01:28:34 am                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import os
import dotenv
import logging
from dataclasses import dataclass

from recsys.config.base import StepPO, FilesetPO, DatasetPO, OperatorParams, DATA_STRUCTURE

# ------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #

# ------------------------------------------------------------------------------------------------ #
#                          MOVIELENS 25M DATA PREP CONFIG                                          #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class CreateDatasetStepPO(StepPO):
    name: str = "create_ratings_dataset"
    description: str = "Creates ratings Dataset object"
    force: bool = False


# ------------------------------------------------------------------------------------------------ #
@dataclass
class CreateDatasetInputPO(FilesetPO):
    source: str = "movielens25m"
    filepath: str = None

    def __post_init__(self) -> None:
        dotenv.load_dotenv()
        ENV = os.getenv("ENV")
        try:
            self.filepath = DATA_STRUCTURE["data"]["sources"][self.source][ENV]
        except KeyError as e:
            msg = f"Data source, {self.datasource} or environment: {ENV} is not recognized.\n{e}"
            logger.error(msg)
            raise ValueError(msg)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class CreateDatasetOutputPO(DatasetPO):
    name: str = "ratings"
    description: str = "Ratings Dataset"
    source: str = "movielens25m"
    stage: str = "staged"
