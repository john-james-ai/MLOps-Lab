#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /data.py                                                                            #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday November 25th 2022 06:28:24 pm                                               #
# Modified   : Sunday November 27th 2022 03:02:21 am                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import os

# ------------------------------------------------------------------------------------------------ #
#                                      FILEPATHS                                                   #
# ------------------------------------------------------------------------------------------------ #
ETL_CONFIG_FILE = "recsys/config/etl.yml"

# ------------------------------------------------------------------------------------------------ #
#                                DATA PROCESSING STAGES                                            #
# ------------------------------------------------------------------------------------------------ #
STAGES = ["staged", "interim", "final"]

# ------------------------------------------------------------------------------------------------ #
#                            DATA TYPES AND FILE FORMATS                                           #
# ------------------------------------------------------------------------------------------------ #
IMMUTABLE_TYPES: tuple = (str, int, float, bool, type(None))
SEQUENCE_TYPES: tuple = (list, tuple)
COMPRESSED_FILE_FORMATS = ("tar.gz", "zip", "7z")


# ------------------------------------------------------------------------------------------------ #
#                                    DIRECTORY CONFIG                                              #
# ------------------------------------------------------------------------------------------------ #
DIRECTORIES = {
    "data": {
        "base": "data",
        "ext": "data/ext",
        "raw": "data/raw",
        "prod": "data/working/prod",
        "dev": "data/working/dev",
        "test": "data/working/test",
    },
    "models": {"base": "models", "prod": "models/prod", "dev": "models/dev", "test": "models/test"},
}
# ------------------------------------------------------------------------------------------------ #
#                                     REPO CONFIG                                                  #
# ------------------------------------------------------------------------------------------------ #
REPO_FILE_FORMAT = "pkl"
REPO_DIRS = {
    "data": {
        "prod": os.path.join(DIRECTORIES["data"]["prod"], "repo"),
        "dev": os.path.join(DIRECTORIES["data"]["dev"], "repo"),
        "test": os.path.join(DIRECTORIES["data"]["test"], "repo"),
    },
    "models": {
        "prod": os.path.join(DIRECTORIES["models"]["prod"], "repo"),
        "dev": os.path.join(DIRECTORIES["models"]["dev"], "repo"),
        "test": os.path.join(DIRECTORIES["models"]["test"], "repo"),
    },
}
# ------------------------------------------------------------------------------------------------ #
#                                     DB CONFIG                                                    #
# ------------------------------------------------------------------------------------------------ #
DB_TABLES = {"DatasetRegistry": "dataset_registry"}
DB_LOCATIONS = {
    "data": {
        "prod": os.path.join(REPO_DIRS["data"]["prod"], "dataset_registry.sqlite"),
        "dev": os.path.join(REPO_DIRS["data"]["dev"], "dataset_registry.sqlite"),
        "test": os.path.join(REPO_DIRS["data"]["test"], "dataset_registry.sqlite"),
    },
    "models": {
        "prod": os.path.join(REPO_DIRS["models"]["prod"], "dataset_registry.sqlite"),
        "dev": os.path.join(REPO_DIRS["models"]["dev"], "dataset_registry.sqlite"),
        "test": os.path.join(REPO_DIRS["models"]["test"], "dataset_registry.sqlite"),
    },
}


# ------------------------------------------------------------------------------------------------ #
#                             SAMPLING AND TRAIN PROPORTIONS                                       #
# ------------------------------------------------------------------------------------------------ #

SAMPLE_PROPORTION = {"prod": 1.0, "dev": 0.1, "test": 0.01}
TRAIN_PROPORTION = 0.8
