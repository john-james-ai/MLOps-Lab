#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/workflow/operators/train_test_split.py                                 #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday December 30th 2022 11:18:59 am                                               #
# Modified   : Saturday January 7th 2023 12:46:38 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
from types import SimpleNamespace
import pandas as pd

from .base import Operator
from recsys.core.entity.dataset import Dataset


# ------------------------------------------------------------------------------------------------ #
#                               TRAIN TEST SPLIT OPERATOR                                          #
# ------------------------------------------------------------------------------------------------ #
class TrainTestSplit(Operator):
    """Produces a sample from a pandas DataFrame, clustering on values of a designated variable.

    Args:

        task_params (dict): Name, description and mode parameters for the Task object.
        operator_params (dict): Parameters that govern the operation, i.e. split variable and the
            train size as proportion of the data to reserve for training.
        input_params (dict): The parameters specifying the input Dataset
        output_params (dict): The Dataset parameters.
        train_params (dict): The parameters specifying the train DataFrame.
        test_params (dict): The parameters specifying the test DataFrame.

    """

    def __init__(self, task_params: dict, operator_params: dict, input_params: dict, output_params: dict, train_params: dict, test_params: dict) -> None:

        self._task_params = SimpleNamespace(**task_params)
        self._operator_params = SimpleNamespace(**operator_params)
        self._input_params = SimpleNamespace(**input_params)
        self._output_params = SimpleNamespace(**output_params)
        self._train_params = SimpleNamespace(**train_params)
        self._test_params = SimpleNamespace(**test_params)

        super().__init__(name=self._task_params.name, mode=self._task_params.mode, description=self._task_params.description)

        self._split_var = self._operator_params.split_var
        self._train_size = self._operator_params.train_size

    def execute(self, *args, **kwargs) -> Dataset:

        task = self._setup()
        dataframe = self._get_dataframe()
        dataset = self._execute(dataframe.data)
        dataset.task_id = task.id
        self._put_dataset(dataset)
        self._teardown(task)

        return dataset

    def _execute(self, data: pd.DataFrame) -> pd.DataFrame:
        """Returns a Dataset containing Training and Test DataFrame objects."""

        sorted_data = data.sort_values(by=self._split_var, ascending=True).reset_index()
        train_idx = sorted_data.index < sorted_data.shape[0] * self._train_size
        test_idx = sorted_data.index >= sorted_data.shape[0] * self._train_size

        train = sorted_data[train_idx]
        test = sorted_data[test_idx]

        dataset = self._build_dataset(train, test)

        return dataset

    def _build_dataset(self, train: pd.DataFrame, test: pd.DataFrame) -> Dataset:
        """Constructs the output Dataset."""
        dataset = Dataset(
            name=self._output_params.name,
            datasource=self._output_params.datasource,
            stage=self._output_params.stage,
            description=self._output_params.description,
            mode=self._output_params.mode
        )
        train_dataframe = dataset.create_dataframe(data=train, name=self._train_params.name, description=self._train_params.description)
        dataset.add_dataframe(train_dataframe)
        test_dataframe = dataset.create_dataframe(data=test, name=self._test_params.name, description=self._test_params.description)
        dataset.add_dataframe(test_dataframe)
        return dataset
