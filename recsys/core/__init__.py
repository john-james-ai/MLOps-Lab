#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /__init__.py                                                                        #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday November 20th 2022 10:55:22 pm                                               #
# Modified   : Wednesday November 23rd 2022 10:29:34 am                                            #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #

# ------------------------------------------------------------------------------------------------ #
#                            DATA TYPES AND FILE FORMATS                                           #
# ------------------------------------------------------------------------------------------------ #
IMMUTABLE_TYPES: tuple = (str, int, float, bool, type(None))
SEQUENCE_TYPES: tuple = (list, tuple)
COMPRESSED_FILE_FORMATS = ("tar.gz", "zip", "7z")
# ------------------------------------------------------------------------------------------------ #
#                                ENVIRONMENT AND STAGE                                             #
# ------------------------------------------------------------------------------------------------ #
ENVS = ["dev", "prod", "test"]
STAGES = ["raw", "interim", "cooked"]
# ------------------------------------------------------------------------------------------------ #
#                                    DATASET CONFIG                                                #
# ------------------------------------------------------------------------------------------------ #
DATASET_FEATURES = [
    "name",
    "description",
    "env",
    "stage",
    "version",
    "cost",
    "nrows",
    "ncols",
    "null_counts",
    "memory_size",
    "filepath",
    "creator",
    "created",
]
# ------------------------------------------------------------------------------------------------ #
#                                     REPO CONFIG                                                  #
# ------------------------------------------------------------------------------------------------ #
REPO_FILE_FORMAT = "pkl"
REPO_DIRS = {
    "data": {
        "dev": "data/movielens20m/repo",
        "prod": "data/movielens20m/repo",
        "test": "tests/data/movielens20m/repo",
    },
    "model": {
        "dev": "models/movielens20m/repo",
        "prod": "models/movielens20m/repo",
        "test": "tests/models/movielens20m/repo",
    },
}
# ------------------------------------------------------------------------------------------------ #
#                                     DB CONFIG                                                    #
# ------------------------------------------------------------------------------------------------ #
DB_TABLES = {"DatasetRegistry": "dataset_registry"}
DB_LOCATIONS = {
    "data": {
        "dev": "data/movielens20m/repo/dataset_registry.sqlite",
        "prod": "data/movielens20m/repo/dataset_registry.sqlite",
        "test": "tests/data/movielens20m/repo/dataset_registry.sqlite",
    },
    "model": {
        "dev": "models/repo/model_registry.sqlite",
        "prod": "models/repo/model_registry.sqlite",
        "test": "tests/models/repo/model_registry.sqlite",
    },
}
