#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Enter Project Name in Workspace Settings                                            #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /mlops_lab/data/movielens25m/config.py                                              #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : Enter URL in Workspace Settings                                                     #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday December 12th 2022 12:32:54 am                                               #
# Modified   : Tuesday January 24th 2023 08:13:48 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Workflow Container Module"""
from dependency_injector import containers, providers  # pragma: no cover


# ------------------------------------------------------------------------------------------------ #
class MovieLens25M(containers.DeclarativeContainer):

    config = providers.Configuration(yaml_files=["./config.yml"])
