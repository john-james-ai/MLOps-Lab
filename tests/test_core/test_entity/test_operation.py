#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /tests/test_core/test_entity/test_operation.py                                      #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday December 17th 2022 03:46:34 am                                             #
# Modified   : Sunday December 18th 2022 06:18:21 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import inspect
from datetime import datetime
import pytest
import logging

from recsys.core.entity.operation import Operation
from recsys.core.dal.dto import OperationDTO
from recsys.core.services.operator import NullOperator, Operator
import tests.containers    # noqa F401

# ------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #


@pytest.mark.operation
class TestOperation:  # pragma: no cover
    # ============================================================================================ #
    def test_setup(self, container, caplog):
        start = datetime.now()
        logger.info(
            "\n\n\tStarted {} {} at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                start.strftime("%I:%M:%S %p"),
                start.strftime("%m/%d/%Y"),
            )
        )
        # ---------------------------------------------------------------------------------------- #
        ts = container.table.operation()
        ts.reset()
        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            "\n\tCompleted {} {} in {} seconds at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                duration,
                end.strftime("%I:%M:%S %p"),
                end.strftime("%m/%d/%Y"),
            )
        )

    # ============================================================================================ #
    def test_instantiation_properties(self, caplog):
        start = datetime.now()
        logger.info(
            "\n\n\tStarted {} {} at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                start.strftime("%I:%M:%S %p"),
                start.strftime("%m/%d/%Y"),
            )
        )
        # ---------------------------------------------------------------------------------------- #
        op = Operation(name=inspect.stack()[0][3], description=f"Description of {inspect.stack()[0][3],}", mode="test", stage="interim", operator=NullOperator(), task_id=9)
        assert op.name == inspect.stack()[0][3]
        assert op.description == f"Description of {inspect.stack()[0][3],}"
        assert op.mode == 'test'
        assert op.stage == 'interim'
        assert isinstance(op.operator, Operator)
        assert op.task_id == 9
        assert op.uri is not None

        with pytest.raises(TypeError):
            op.task_id = 10

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            "\n\tCompleted {} {} in {} seconds at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                duration,
                end.strftime("%I:%M:%S %p"),
                end.strftime("%m/%d/%Y"),
            )
        )

    # ============================================================================================ #
    def test_as_dto(self, caplog):
        start = datetime.now()
        logger.info(
            "\n\n\tStarted {} {} at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                start.strftime("%I:%M:%S %p"),
                start.strftime("%m/%d/%Y"),
            )
        )
        # ---------------------------------------------------------------------------------------- #
        op = Operation(name=inspect.stack()[0][3], description=f"Description of {inspect.stack()[0][3],}", mode="test", stage="interim", operator=NullOperator(), task_id=9)
        dto = op.as_dto()
        assert isinstance(dto, OperationDTO)
        assert dto.id is None
        assert dto.name == inspect.stack()[0][3]
        assert dto.description == f"Description of {inspect.stack()[0][3],}"
        assert dto.mode == 'test'
        assert dto.stage == 'interim'
        assert dto.task_id == 9
        assert dto.uri is not None
        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            "\n\tCompleted {} {} in {} seconds at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                duration,
                end.strftime("%I:%M:%S %p"),
                end.strftime("%m/%d/%Y"),
            )
        )

    # ============================================================================================ #
    def test_as_dict(self, caplog):
        start = datetime.now()
        logger.info(
            "\n\n\tStarted {} {} at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                start.strftime("%I:%M:%S %p"),
                start.strftime("%m/%d/%Y"),
            )
        )
        # ---------------------------------------------------------------------------------------- #
        op = Operation(name=inspect.stack()[0][3], description=f"Description of {inspect.stack()[0][3],}", mode="test", stage="interim", operator=NullOperator(), task_id=9)
        dto = op.as_dict()
        assert isinstance(dto, dict)
        assert dto['id'] is None
        assert dto['name'] == inspect.stack()[0][3]
        assert dto['description'] == f"Description of {inspect.stack()[0][3],}"
        assert dto['mode'] == 'test'
        assert dto['stage'] == 'interim'
        assert dto['operator'] is None
        assert dto['task_id'] == 9
        assert dto['uri'] is not None
        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            "\n\tCompleted {} {} in {} seconds at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                duration,
                end.strftime("%I:%M:%S %p"),
                end.strftime("%m/%d/%Y"),
            )
        )

    # ============================================================================================ #
    def test_from_dto(self, caplog):
        start = datetime.now()
        logger.info(
            "\n\n\tStarted {} {} at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                start.strftime("%I:%M:%S %p"),
                start.strftime("%m/%d/%Y"),
            )
        )
        # ---------------------------------------------------------------------------------------- #
        op1 = Operation(name=inspect.stack()[0][3], description=f"Description of {inspect.stack()[0][3],}", mode="test", stage="interim", operator=NullOperator(), task_id=9)
        dto = op1.as_dto()
        op2 = Operation.from_dto(dto)
        assert op1.id == op2.id
        assert op1.name == op2.name
        assert op1.description == op2.description
        assert op1.mode == op2.mode
        assert op1.stage == op2.stage
        assert op1.uri == op2.uri
        assert op1.task_id == op2.task_id
        assert op1.created == op2.created
        assert op1.modified == op2.modified

        op3 = {'some': 'dict'}
        assert not op1 == op3

        op4 = Operation(name=inspect.stack()[0][3], description=f"Description of {inspect.stack()[0][3],}", mode="test", stage="interim", operator=NullOperator(), task_id=None)
        op4.task_id = 22
        assert not op1 == op4

        with pytest.raises(TypeError):
            op4.task_id = 99

        op4.task_id = 22

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            "\n\tCompleted {} {} in {} seconds at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                duration,
                end.strftime("%I:%M:%S %p"),
                end.strftime("%m/%d/%Y"),
            )
        )

    # ============================================================================================ #
    def test_validation(self, caplog):
        start = datetime.now()
        logger.info(
            "\n\n\tStarted {} {} at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                start.strftime("%I:%M:%S %p"),
                start.strftime("%m/%d/%Y"),
            )
        )
        # ---------------------------------------------------------------------------------------- #
        with pytest.raises(ValueError):
            _ = Operation(
                name=inspect.stack()[0][3],
                description=f"Description of {inspect.stack()[0][3],}",
                mode="029",
                stage="interim",
                operator=NullOperator(),
                task_id=9)

        with pytest.raises(ValueError):
            _ = Operation(
                name=inspect.stack()[0][3],
                description=f"Description of {inspect.stack()[0][3],}",
                mode="test",
                stage="interim77",
                operator=NullOperator(),
                task_id=9)

        with pytest.raises(TypeError):
            _ = Operation(
                name=inspect.stack()[0][3],
                description=f"Description of {inspect.stack()[0][3],}",
                mode="test",
                stage="interim",
                operator=9,
                task_id=9)

        with pytest.raises(TypeError):
            _ = Operation(
                name=inspect.stack()[0][3],
                description=f"Description of {inspect.stack()[0][3],}",
                mode="test",
                stage="interim",
                operator=NullOperator(),
                task_id='a')
        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            "\n\tCompleted {} {} in {} seconds at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                duration,
                end.strftime("%I:%M:%S %p"),
                end.strftime("%m/%d/%Y"),
            )
        )

    # ============================================================================================ #
    def test_magic(self, caplog):
        start = datetime.now()
        logger.info(
            "\n\n\tStarted {} {} at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                start.strftime("%I:%M:%S %p"),
                start.strftime("%m/%d/%Y"),
            )
        )
        # ---------------------------------------------------------------------------------------- #
        op1 = Operation(name=inspect.stack()[0][3], description=f"Description of {inspect.stack()[0][3],}", mode="test", stage="interim", operator=NullOperator(), task_id=9)
        assert isinstance(op1.__str__(), str)
        assert isinstance(op1.__repr__(), str)

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            "\n\tCompleted {} {} in {} seconds at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                duration,
                end.strftime("%I:%M:%S %p"),
                end.strftime("%m/%d/%Y"),
            )
        )
