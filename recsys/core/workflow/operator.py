#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/workflow/operator.py                                                   #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday December 5th 2022 02:31:12 am                                                #
# Modified   : Monday December 19th 2022 03:32:02 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Operator Entity Module"""
from abc import ABC, abstractmethod
import time
import pandas as pd
import numpy as np
import logging
import urllib
import tarfile
from zipfile import ZipFile
from typing import Dict

# ================================================================================================ #
#                                    OPERATOR BASE CLASS                                           #
# ================================================================================================ #


class Operator(ABC):
    """Operator Base Class"""
    def __init__(self, *args, **kwargs) -> None:
        self._logger = logging.getLogger(
            f"{self.__module__}.{self.__class__.__name__}",
        )

    def __str__(self) -> str:
        return f"Operator Name: {self.__class__.__name__}, Module: {self.__module__}"

    def __repr__(self) -> str:
        return f"{self.__class__.__name__},{self.__module__}"

    @abstractmethod
    def execute(self, *args, **kwargs) -> None:
        """Executes the operation."""

# ================================================================================================ #
#                                    DOWNLOAD OPERATOR                                             #
# ================================================================================================ #


class DownloadExtractor(Operator):
    """Downloads and if compressed, extracts data from a website.
    Args:
        url (str): The URL to the web resource
        destination (str): For compressed file sources, this must be the directory into which
            the data is to land. For non-compressed sources, this must be a path to the
            file to be downloaded.
    """

    def __init__(self, url: str, destination: str) -> None:
        super().__init__()
        self._url = url
        self._destination = destination

    def execute(self, *args, **kwargs) -> None:
        """Download file."""
        if ".zip" in self._url:
            self._execute_download_zip()
        elif ".tar.gz" in self._url:
            self._execute_download_tar_gz()
        else:
            self._execute_download()

    def _execute_download_zip(self) -> None:
        """Downloads and extracts the data from a zip file."""
        # Open the url
        zipresp = urllib.request.urlopen(self._url)
        # Create a new file on the hard drive
        tempzip = open("/tmp/tempfile.zip", "wb")
        # Write the contents of the downloaded file into the new file
        tempzip.write(zipresp.read())
        # Close the newly-created file
        tempzip.close()
        # Re-open the newly-created file with ZipFile()
        zf = ZipFile("/tmp/tempfile.zip")
        # Extract its contents into <extraction_path>
        # note that extractall will automatically create the path
        zf.extractall(path=self._destination)
        # close the ZipFile instance
        zf.close()

    def _execute_download_tar_gz(self) -> None:
        """Downloads and extracts the data from a .tar.gz file."""
        # Open the url
        ftpstream = urllib.request.urlopen(self._url)
        # Create a new file on the hard drive
        targz_file = tarfile.open(fileobj=ftpstream, mode="r|gz")
        # Extract its contents into <extraction_path>
        targz_file.extractall(path=self._destination)

    def _execute_download(self) -> None:
        """Downloads a file from a remote source."""
        try:
            _ = urllib.request.urlretrieve(url=self._url, filename=self._destination)
        except IsADirectoryError:
            msg = "The destination parameter is a directory. For download, this must be a path to a file."
            self._logger.error(msg)
            raise IsADirectoryError(msg)

# ------------------------------------------------------------------------------------------------ #
#                                          PICKLER                                                 #
# ------------------------------------------------------------------------------------------------ #


class NullOperator(Operator):
    """Null Operator does nothing. Returns the data it receives from the Environment.

    Args:
        seconds (int): Number of seconds the operator should take, i.e. sleep.

    """

    def __init__(self, seconds: int = 2, *args, **kwargs) -> None:
        super().__init__()
        self._seconds = seconds

    def execute(self, data: pd.DataFrame, *args, **kwargs) -> None:
        """Executes the operation"""
        time.sleep(self._seconds)
        return data

# ------------------------------------------------------------------------------------------------ #
#                                          SAMPLER                                                 #
# ------------------------------------------------------------------------------------------------ #


class Sampler(Operator):
    """Copies a file from datasource to destination
    Args:
        cluster (bool): Conduct cluster sampling if True. Otherwise, simple random sampling.
        cluster_by (str): The column name to cluster by.
        frac (float): The proportion of the data to return as sample.
        replace (bool): Whether to sample with replacement. Default = False.
        shuffle (bool): Whether to shuffle before sampling. Default = True.
        random_state (int): The pseudo random seed for reproducibility.
    """

    def __init__(self, cluster: bool = False, cluster_by: str = None, frac: float = None, replace: bool = False, shuffle: bool = True,
                 random_state: int = None) -> None:
        super().__init__()
        self._cluster = cluster
        self._cluster_by = cluster_by
        self._frac = frac
        self._replace = replace
        self._shuffle = shuffle
        self._random_state = random_state

    def execute(self, data: pd.DataFrame, *args, **kwargs) -> None:
        """Executes the operation on the DataFrame
        Args:
            data (pd.DataFrame): The DataFrame to be sampled.
        """

        if self._cluster:
            data = self._sample_by_cluster(data)
        else:
            data = data.sample(frac=self._frac, random_state=self._random_state)
        return data

    def _sample_by_cluster(self, data: pd.DataFrame) -> pd.DataFrame:
        """Returns a sample of clusters."""
        if self._frac == 1:
            return data
        elif self._frac > 1:
            msg = "The frac parameter must be in (0,1]"
            self._logger.error(msg)
            raise ValueError(msg)

        rng = np.random.default_rng(self._random_state)

        try:
            clusters = data[self._cluster_by].unique()
            n_clusters = len(clusters)
            size = int(n_clusters * self._frac)
            sample_clusters = rng.choice(a=clusters, size=size, replace=self._replace, shuffle=self._shuffle)
            sample = data.loc[data[self._cluster_by].isin(sample_clusters)]
            return sample
        except KeyError:
            msg = "The dataframe has no column {}".format(self._cluster_by)
            self._logger.error(msg)
            raise KeyError(msg)


# ------------------------------------------------------------------------------------------------ #
#                               TRAIN TEST SPLIT OPERATOR                                          #
# ------------------------------------------------------------------------------------------------ #
class TrainTestSplit(Operator):
    """Splits the dataset into to training and test sets using global
    Args:
        split_var (str): The variable containing the temporal value.
        train_size (float): The proportion of the data to allocate to the training set.

    Returns: Dictionary of Train and Test Dataset objects.
    """

    def __init__(self, split_var: str, train_size: float = 0.80) -> None:
        super().__init__()
        self._split_var = split_var
        self._train_size = train_size

    def execute(self, data: pd.DataFrame, *args, **kwargs) -> Dict[str, pd.DataFrame]:

        sorted_data = data.sort_values(by=self._split_var, ascending=True).reset_index()
        train_idx = sorted_data.index < sorted_data.shape[0] * self._train_size
        test_idx = sorted_data.index >= sorted_data.shape[0] * self._train_size

        train = sorted_data[train_idx]
        test = sorted_data[test_idx]

        result = {"train": train, "test": test}

        return result


# ------------------------------------------------------------------------------------------------ #
#                               DATA CENTRALIZER OPERATOR                                          #
# ------------------------------------------------------------------------------------------------ #


class DataCenterizer(Operator):
    """Centers the a continuous variable by the mean of a centering variable.

    Args:
        var (str): The name of the column to center by subtracting the mean by group_var.
        group_var (str): The name of the column upon which the data are grouped
        out_var (str): The name of the column to contain the centered var values.

    Returns: pd.DataFrame
    """

    def __init__(self, var: str, group_var: str, out_var: str) -> None:
        super().__init__()
        self._var = var
        self._group_var = group_var
        self._out_var = out_var

    def execute(self, data: pd.DataFrame, *args, **kwargs) -> pd.DataFrame:

        data[self._out_var] = data[self._var].sub(
            data.groupby(self._group_var)[self._var].transform("mean")
        )

        return data


# ------------------------------------------------------------------------------------------------ #
#                                DATA AGGREGATOR OPERATOR                                          #
# ------------------------------------------------------------------------------------------------ #


class MeanAggregator(Operator):
    """Aggregates data by computing the mean of a variable by a grouping another variable.

    Args:
        var (str): The name of the variable to which the mean statistic is applied.
        group_var (str): The name of the column upon which the data are grouped
        out_var (str): The name of the column to hold the aggregated mean.

    Returns: pd.DataFrame
    """

    def __init__(self, var: str, group_var: str, out_var: str) -> None:
        super().__init__()
        self._var = var
        self._group_var = group_var
        self._out_var = out_var

    def execute(self, data: pd.DataFrame, *args, **kwargs) -> pd.DataFrame:

        result = data.groupby(self._group_var)[self._var].mean().reset_index()
        result.columns = [self._group_var, self._out_var]

        return result
