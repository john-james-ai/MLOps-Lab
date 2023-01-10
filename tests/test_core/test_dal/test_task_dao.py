#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Taskname   : /tests/test_core/test_dal/test_task_dao.py                                          #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday December 28th 2022 02:38:04 pm                                            #
# Modified   : Tuesday January 10th 2023 01:44:36 am                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import inspect
from datetime import datetime
import pytest
import logging
import mysql.connector

from recsys.core.dal.dto import DTO
from recsys.core.dal.dao import DAO
from recsys.core.entity.job import Task

# ------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #
double_line = f"\n{100 * '='}"
single_line = f"\n{100 * '-'}"


@pytest.mark.dao
@pytest.mark.task_dao
class TestTaskDAO:  # pragma: no cover

    def reset_table(self, container):
        dba = container.dba.task()
        dba.reset()

    def get_dao(self, container) -> DAO:
        dao = container.dal.task()
        return dao

    # ============================================================================================ #
    def check_results(self, i, dto) -> None:
        assert (dto.id == i or dto.id == 5 + i)
        assert isinstance(dto, DTO)
        assert "task_" in dto.name
        assert dto.mode == 'test'

    # ============================================================================================ #
    def test_setup(self, container, caplog):
        start = datetime.now()
        logger.info(
            "\n\nStarted {} {} at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                start.strftime("%I:%M:%S %p"),
                start.strftime("%m/%d/%Y"),
            )
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        self.reset_table(container)
        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            "\nCompleted {} {} in {} seconds at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                duration,
                end.strftime("%I:%M:%S %p"),
                end.strftime("%m/%d/%Y"),
            )
        )
        logger.info(single_line)

    # ================================================= =========================================== #
    def test_create_exists(self, container, jobs, caplog):
        start = datetime.now()
        logger.info(
            "\n\nStarted {} {} at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                start.strftime("%I:%M:%S %p"),
                start.strftime("%m/%d/%Y"),
            )
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        dao = self.get_dao(container)
        dao.begin()

        for name, task in jobs[0].tasks.items():
            dto = task.as_dto()
            dto = dao.create(dto)
            assert isinstance(task, Task)

        for i in range(1, 6):
            assert dao.exists(i)

        dao.rollback()

        dao.begin()

        for i in range(1, 6):
            assert not dao.exists(i)

        dao.close()

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
        logger.info(single_line)

    # ============================================================================================ #
    def test_create_commit(self, container, jobs, caplog):
        start = datetime.now()
        logger.info(
            "\n\nStarted {} {} at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                start.strftime("%I:%M:%S %p"),
                start.strftime("%m/%d/%Y"),
            )
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        dao = self.get_dao(container)

        for name, task in jobs[1].tasks.items():
            dto = task.as_dto()
            dto = dao.create(dto)
            assert isinstance(task, Task)

        dao.rollback()

        for i in range(6, 11):
            assert dao.exists(i)
            logger.debug(dao.read(i))
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
        logger.info(single_line)

    # ============================================================================================ #
    def test_read(self, container, caplog):
        start = datetime.now()
        logger.info(
            "\n\nStarted {} {} at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                start.strftime("%I:%M:%S %p"),
                start.strftime("%m/%d/%Y"),
            )
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        dao = self.get_dao(container)

        for i in range(6, 11):
            dto = dao.read(i)
            assert isinstance(dto, DTO)
            self.check_results(i, dto)

        dto = dao.read(99)
        assert dto == []
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
        logger.info(single_line)

    # ============================================================================================ #
    def test_read_all(self, container, caplog):
        start = datetime.now()
        logger.info(
            "\n\nStarted {} {} at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                start.strftime("%I:%M:%S %p"),
                start.strftime("%m/%d/%Y"),
            )
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        dao = self.get_dao(container)

        dtos = dao.read_all()
        assert isinstance(dtos, dict)
        for i, dto in dtos.items():
            self.check_results(i, dto)

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
        logger.info(single_line)

    # ============================================================================================ #
    def test_read_by_name_mode(self, container, caplog):
        start = datetime.now()
        logger.info(
            "\n\nStarted {} {} at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                start.strftime("%I:%M:%S %p"),
                start.strftime("%m/%d/%Y"),
            )
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        dao = self.get_dao(container)

        dto = dao.read_by_name_mode(name="task_1_job_2", mode='test')
        assert isinstance(dto, DTO)
        self.check_results(1, dto)

        dto = dao.read_by_name_mode(name="task_1", mode='skdi')
        assert dto == []

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
        logger.info(single_line)

    # ============================================================================================ #
    def test_read_by_parent(self, container, jobs, caplog):
        start = datetime.now()
        logger.info(
            "\n\nStarted {} {} at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                start.strftime("%I:%M:%S %p"),
                start.strftime("%m/%d/%Y"),
            )
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        dao = self.get_dao(container)
        for i in range(6, 11):
            tasks = dao.read_by_parent_id(i)
            for id, task in tasks.items():
                assert isinstance(task, Task)
                assert task.parent_id == i

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
        logger.info(single_line)

    # ============================================================================================ #
    def test_update(self, container, caplog):
        start = datetime.now()
        logger.info(
            "\n\nStarted {} {} at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                start.strftime("%I:%M:%S %p"),
                start.strftime("%m/%d/%Y"),
            )
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        dao = self.get_dao(container)
        dao.begin()
        dtos = dao.read_all()
        for i, dto in dtos.items():
            dto.state = "READY"
            dao.update(dto)

        dao.rollback()

        dtos = dao.read_all()
        for i, dto in dtos.items():
            assert not dto.state == 'READY'

        dao.begin()
        for i, dto in dtos.items():
            dto.state = "READY"
            dao.update(dto)
        dao.save()

        dtos = dao.read_all()
        for i, dto in dtos.items():
            assert dto.state == 'READY'
            dto.id = 8938
            with pytest.raises(mysql.connector.ProgrammingError):
                dao.update(dto)
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
        logger.info(single_line)

    # ============================================================================================ #
    def test_delete(self, container, caplog):
        start = datetime.now()
        logger.info(
            "\n\nStarted {} {} at {} on {}".format(
                self.__class__.__name__,
                inspect.stack()[0][3],
                start.strftime("%I:%M:%S %p"),
                start.strftime("%m/%d/%Y"),
            )
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        dao = self.get_dao(container)
        dao.begin()
        dao.delete(7)
        assert not dao.exists(7)

        dao.rollback()

        assert dao.exists(7)
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
        logger.info(single_line)
