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
# Modified   : Tuesday November 22nd 2022 02:37:40 am                                              #
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
DB_LOCATIONS = {
    "dev": "data/movielens20m/repo/registry.sqlite",
    "prod": "data/movielens20m/repo/registry.sqlite",
    "test": "tests/data/movielens20m/repo/registry.sqlite",
}
