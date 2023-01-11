#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/services/operators/base.py                                             #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday December 4th 2022 08:30:24 pm                                                #
# Modified   : Wednesday January 11th 2023 03:05:49 am                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
from abc import abstractmethod
from typing import Union

from recsys.core.entity.base import Entity
from ..base import Service


# ================================================================================================ #
#                                    OPERATOR BASE CLASS                                           #
# ================================================================================================ #
class Operator(Service):
    """Operator Base Class"""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__()
        # self._name = name
        # self._description = description

    # def __str__(self) -> str:
    #     return f"Operator:\n\tModule: {self.__module__}\n\tClass: {self.__class__.__name__}\n\tName: {self._name}\n\tDescription: {self._description}"

    # def __repr__(self) -> str:
    #     return f"{self.__module__}, {self.__class__.__name__}, {self._name}, {self._description}"

    @property
    def name(self) -> str:
        return self._name

    @property
    def description(self) -> str:
        return self._description

    @abstractmethod
    def execute(self, *args, **kwargs) -> Union[None, Entity]:
        """Executes the operation."""
