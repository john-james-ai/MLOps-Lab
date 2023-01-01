#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /recsys/core/services/validation.py                                                 #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday December 27th 2022 02:41:20 pm                                              #
# Modified   : Sunday January 1st 2023 06:10:31 am                                                 #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Validation Module."""
from dataclasses import dataclass
import pandas as pd

from recsys import SOURCES, MODES, STAGES, STATES


# ------------------------------------------------------------------------------------------------ #
@dataclass
class Response:
    is_ok: bool = True
    msg: str = None
    exception: type(Exception) = ValueError


# ------------------------------------------------------------------------------------------------ #
class Validator:
    """Provides validates entities."""

    @classmethod
    def datasource(cls, entity) -> Response:
        response = Response()
        if hasattr(entity, 'datasource_name'):
            if entity.datasource_name not in SOURCES:
                response.is_ok = False
                response.msg = f"Error in {entity.__class__.__name__}. Variable 'datasource_name' is not valid. Must be one of {SOURCES}."
        return response

    @classmethod
    def mode(cls, entity) -> Response:
        response = Response()
        if hasattr(entity, 'mode'):
            if entity.mode not in MODES:
                response.is_ok = False
                response.msg = f"Error in {entity.__class__.__name__}. Variable 'mode' is not valid. Must be one of {MODES}."
        return response

    @classmethod
    def stage(cls, entity) -> Response:
        response = Response()
        if hasattr(entity, 'stage'):
            if entity.stage not in STAGES:
                response.is_ok = False
                response.msg = f"Error in {entity.__class__.__name__}. Variable 'stage' is not valid. Must be one of {STAGES}."
        return response

    @classmethod
    def data(cls, entity) -> Response:
        response = Response()
        if hasattr(entity, 'data'):
            if not isinstance(entity.data, pd.DataFrame) and entity.data is not None:
                response.is_ok = False
                response.msg = f"Error in {entity.__class__.__name__}. Variable 'data' is not valid. Must be a pandas DataFrame object."
                response.exception = TypeError
        return response

    @classmethod
    def job_id(cls, entity) -> Response:
        response = Response()
        if hasattr(entity, 'job_id'):
            if not isinstance(entity.job_id, int) and entity.job_id is not None:
                response.is_ok = False
                response.msg = f"Error in {entity.__class__.__name__}. Variable 'job_id' is not valid. Must be an integer."
                response.exception = TypeError
        return response

    @classmethod
    def task_id(cls, entity) -> Response:
        response = Response()
        if hasattr(entity, 'task_id'):
            if not isinstance(entity.task_id, int) and entity.task_id is not None:
                response.is_ok = False
                response.msg = f"Error in {entity.__class__.__name__}. Variable 'task_id' is not valid. Must be an integer."
                response.exception = TypeError
        return response

    @classmethod
    def state(cls, entity) -> Response:
        response = Response()
        if hasattr(entity, 'state'):
            if entity.state not in STATES:
                response.is_ok = False
                response.msg = f"Error in {entity.__class__.__name__}. Variable 'state' is not valid. Must be one of {STATES}."
        return response

    def validate(self, entity) -> Response:
        validators = [self.datasource, self.mode, self.stage, self.data, self.job_id, self.task_id, self.state]
        response = Response()
        for validator in validators:
            response = validator(entity)
            if not response.is_ok:
                return response
        return response
