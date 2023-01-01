#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/workflow/operators/data_center.py                                      #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday December 30th 2022 02:04:13 pm                                               #
# Modified   : Saturday December 31st 2022 05:49:43 pm                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Data Center Module"""
from types import SimpleNamespace
import pandas as pd

from .base import Operator
from recsys.core.dal.base import Dataset


# ------------------------------------------------------------------------------------------------ #
#                                  DATA CENTER OPERATOR                                            #
# ------------------------------------------------------------------------------------------------ #
class DataCenter(Operator):
    """Centers a continuous variable by the mean of the centering variable.

    Args:
        task_params (dict): Name, description and mode parameters for the Task object.
        operator_params (dict): The column to center, the centering variable, and the column
            which will contain the centered variable values.
        input_params (dict): The parameters specifying the input Dataset
        output_params (dict): The output Dataset parameters.

    Returns: pd.DataFrame
    """

    def __init__(self, task_params: dict, operator_params: dict, input_params: dict, output_params: dict) -> None:

        self._task_params = SimpleNamespace(**task_params)
        self._operator_params = SimpleNamespace(**operator_params)
        self._input_params = SimpleNamespace(**input_params)
        self._output_params = SimpleNamespace(**output_params)

        super().__init__(name=self._task_params.name, mode=self._task_params.mode, description=self._task_params.description)

        self._var = self._operator_params.var
        self._group_var = self._operator_params.group_var
        self._out_var = self._operator_params.out_var

    def execute(self, *args, **kwargs) -> Dataset:

        task = self._setup()
        dataframe = self._get_dataframe()
        dataset = self._execute(dataframe.data)
        dataset.task_id = task.id
        self._put_dataset(dataset)
        self._teardown(task)

        return dataset

    def _get_data(self) -> pd.DataFrame:
        """Retrieves a pandas DataFrame from the Dataset repository."""
        dataset = self._uow.dataset.get(self._input_params.id)
        dataframe = dataset.get_dataframe(name=dataset.name)
        return dataframe.data

    def _execute(self, data: pd.DataFrame) -> pd.DataFrame:
        """Returns the centered Dataset object."""

        data[self._out_var] = data[self._var].sub(
            data.groupby(self._group_var)[self._var].transform("mean")
        )

        dataset = self._build_dataset(data=data)

        return dataset

    def _build_dataset(self, data: pd.DataFrame) -> Dataset:
        """Constructs the output Dataset."""
        dataset = Dataset(
            name=self._output_params.name,
            datasource=self._output_params.datasource,
            stage=self._output_params.stage,
            description=self._output_params.description,
            mode=self._output_params.mode,
            data=data,

        )

        return dataset
