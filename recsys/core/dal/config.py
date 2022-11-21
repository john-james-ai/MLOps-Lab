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
# Created    : Monday November 14th 2022 01:22:05 am                                               #
# Modified   : Sunday November 20th 2022 12:07:47 am                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Dataset Repository configuration module."""
import os
import logging
from dataclasses import dataclass

from recsys.core.base.config import Config, TEST_REPO_DIR, DATA_REPO_FILE_FORMAT
from recsys.core.services.io import IOService

# ------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #


@dataclass
class DatasetRepoConfig(Config):
    directory: str = TEST_REPO_DIR


@dataclass
class DatasetRepoConfigFR(DatasetRepoConfig):
    """Configuration for Dataset repositories with file-based registries.

    Args:
        name (str): Name of configuration. (Inherited)
        test (bool): Indicates whether the configuration is for testing.
            Default = False (Inherited)
        directory (str): Directory where repo is stored.
            (Inherited)
        file_format (str): Format in which files are stored.
        io (IOService): The IOService class type.
    """

    file_format: str = DATA_REPO_FILE_FORMAT
    io: type(IOService) = IOService

    def __post_init__(self) -> None:
        if self.test:
            self.directory = os.path.join("tests", self.directory)
