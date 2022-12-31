#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/workflow/operators/null_operator.py                                    #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday December 30th 2022 10:33:34 am                                               #
# Modified   : Friday December 30th 2022 10:35:19 am                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import time

from recsys.core.workflow.base import Operator


# ------------------------------------------------------------------------------------------------ #
#                                     NULL OPERATOR                                                #
# ------------------------------------------------------------------------------------------------ #
class NullOperator(Operator):
    """Null Operator does nothing. Returns the data it receives from the Environment.

    Args:
        seconds (int): Number of seconds the operator should take, i.e. sleep.

    """

    def __init__(self, seconds: int = 2, *args, **kwargs) -> None:
        super().__init__()
        self._seconds = seconds

    def execute(self) -> None:
        """Executes the operation"""
        time.sleep(self._seconds)
