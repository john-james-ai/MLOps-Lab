#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /visual.py                                                                          #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday November 29th 2022 02:42:46 pm                                              #
# Modified   : Tuesday November 29th 2022 02:44:04 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Visualization Configuration Module"""
from dataclasses import dataclass

from recsys.config.base import Config


# ------------------------------------------------------------------------------------------------ #
@dataclass
class VisualConfig(Config):
    figsize: tuple = (8, 4)
    darkblue: str = "#1C3879"
    lightblue: str = "steelblue"
    palette: str = "Blues_r"
    style: str = "whitegrid"
