#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/services/task.py                                                       #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday December 12th 2022 12:58:00 pm                                               #
# Modified   : Tuesday December 13th 2022 09:26:49 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
from abc import abstractmethod
import pandas as pd
from typing import Union

from recsys.core.services.base import Service
from recsys.core.workflow.operator import Operator
# ------------------------------------------------------------------------------------------------ #


class Task(Service):
    """Base Class for objects responsible for performing a single task within a pipeline job.

    Args:
        name (str): The name for the task
        description (str): Description of task.
        operator (Operator): An instance of the operator that executes the task.
        input_params (dict): Dictionary describing the input data required. Optional
        output_params (dict): The specification for the output Dataset
        operator_params (dict): Dictionary containing the parameters passed to the operator.
    """

    def __init__(self, name: str, description: str, operator: Operator, input_params: dict, output_params: dict, operator_params: dict) -> None:
        super().__init__()
        self._name = name
        self._description = description
        self._operator = operator
        self._input_params = input_params
        self._output_params = output_params
        self._operator_params = operator_params

        self._input_data = None

    @property
    def input_data(self) -> pd.DataFrame:
        return self._input_data

    @input_data.setter
    def input_data(self, input_data: pd.DataFrame) -> None:
        self._input_data = input_data

    @abstractmethod
    def run(self) -> Union[pd.DataFrame, None]:
        """Runs the operator that fulfills the task."""


