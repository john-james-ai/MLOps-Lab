#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Enter Project Name in Workspace Settings                                            #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /mlops_lab/core/workflow/operator/null_operator.py                                  #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : Enter URL in Workspace Settings                                                     #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday December 30th 2022 10:33:34 am                                               #
# Modified   : Tuesday January 24th 2023 08:13:47 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import time

from .base import Operator


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
