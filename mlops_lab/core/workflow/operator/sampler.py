#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Enter Project Name in Workspace Settings                                            #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /mlops_lab/core/workflow/operator/sampler.py                                        #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : Enter URL in Workspace Settings                                                     #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday December 30th 2022 10:35:54 am                                               #
# Modified   : Tuesday January 24th 2023 08:13:48 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
from types import SimpleNamespace
import pandas as pd
import numpy as np

from .base import Operator
from mlops_lab.core.entity.dataset import Dataset

# ------------------------------------------------------------------------------------------------ #
#                                          SAMPLER                                                 #
# ------------------------------------------------------------------------------------------------ #


class Sampler(Operator):
    """Produces a sample from a pandas DataFrame, clustering on values of a designated variable.

    Args:

        task_params (dict): Name, description and mode parameters for the Task object.
        operator_params (dict): Parameters that govern the operation
        input_params (dict): The parameters specifying the input Dataset
        output_params (dict): The parameters specifying the output Dataset.

        Operation Params include:
            dataset_spec (dict): Specification for the Dataset object to create from the sample.
            mode (str): The mode in which the operation is executed.
            description (str): Operation description
            cluster (bool): Conduct cluster sampling if True. Otherwise, simple random sampling.
            cluster_by (str): The column name to cluster by.
            frac (float): The proportion of the data to return as sample.
            replace (bool): Whether to sample with replacement. Default = False.
            shuffle (bool): Whether to shuffle before sampling. Default = True.
            random_state (int): The pseudo random seed for reproducibility.
    """

    def __init__(
        self, task_params: dict, operator_params: dict, input_params: dict, output_params: dict
    ) -> None:

        self._task_params = SimpleNamespace(**task_params)
        self._operator_params = SimpleNamespace(**operator_params)
        self._input_params = SimpleNamespace(**input_params)
        self._output_params = SimpleNamespace(**output_params)

        super().__init__(
            name=self._task_params.name,
            mode=self._task_params.mode,
            description=self._task_params.description,
        )

        self._cluster = operator_params.cluster
        self._cluster_by = operator_params.cluster_by
        self._frac = operator_params.frac
        self._replace = operator_params.replace
        self._shuffle = operator_params.shuffle
        self._random_state = operator_params.random_state

    def execute(self, *args, **kwargs) -> None:
        """Executes the operation on the DataFrame object"""

        task = self._setup()
        dataframe = self._get_dataframe()
        dataset = self._execute(dataframe.data)
        dataset.task_id = task.id
        self._put_dataset(dataset)
        self._teardown(task)

    def _execute(self, data: pd.DataFrame) -> pd.DataFrame:
        """Returns a clustered sample of the data."""
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
            sample_clusters = rng.choice(
                a=clusters, size=size, replace=self._replace, shuffle=self._shuffle
            )
            sample = data.loc[data[self._cluster_by].isin(sample_clusters)]
            return self._build_dataset(data=sample)
        except KeyError:
            msg = "The dataframe has no column {}".format(self._cluster_by)
            self._logger.error(msg)
            raise KeyError(msg)

    def _build_dataset(self, data: pd.DataFrame) -> None:
        dataset = Dataset(
            name=self._output_params.name,
            datasource=self._output_params.datasource,
            stage=self._output_params.stage,
            description=self._output_params.description,
            mode=self._output_params.mode,
            data=data,
        )
        return dataset
