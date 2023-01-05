#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /tests/test_core/test_dal/test_datasource_url_dao.py                                #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday December 28th 2022 02:38:04 pm                                            #
# Modified   : Wednesday January 4th 2023 05:15:32 pm                                              #
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


# ------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #
double_line = f"\n{100 * '='}"
single_line = f"\n{100 * '-'}"


@pytest.mark.dao
@pytest.mark.datasource_url_dao
class TestDataSourceURLDAO:  # pragma: no cover
    # ============================================================================================ #
    def reset_table(self, container):
        db = container.database.recsys()
        dba = container.dba.datasource_url()
        dba.database = db
        dba.reset()
        dba.database.save()
        dba.database.connection.close()

    # ---------------------------------------------------------------------------------------- #
    def get_dao(self, container) -> DAO:
        db = container.database.recsys()
        dao = container.dao.datasource_url()
        dao.database = db
        return dao

    # ---------------------------------------------------------------------------------------- #
    def check_results(self, dto) -> None:
        assert dto.id is not None
        assert "www.url_" in dto.url
        assert isinstance(dto, DTO)

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

    # ============================================================================================ #
    def test_create_exists(self, container, datasources, caplog):
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
        dao.database.begin()

        for j, (name, datasource_url) in enumerate(datasources[0].urls.items(), start=1):
            dto = datasource_url.as_dto()
            dto.datasource_id = j
            dto = dao.create(dto)
            self.check_results(dto)
            logger.debug(dto)

        for i in range(1, 6):
            exists = dao.exists(i)
            logger.debug(exists)
            assert exists

        cnx = dao.database.connection
        cnx.rollback()

        for i in range(1, 6):
            exists = dao.exists(i)
            assert not exists
        dao.database.save()
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
    def test_create_commit(self, container, datasources, caplog):
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

        for j, (name, datasource_url) in enumerate(datasources[0].urls.items(), start=1):
            dto = datasource_url.as_dto()
            dto.datasource_id = j
            dto = dao.create(dto)
            self.check_results(dto)
            logger.debug(dto)

        cnx = dao.database.connection
        cnx.rollback()

        for i in range(6, 10):
            assert dao.exists(i)

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

        for i in range(6, 10):
            dto = dao.read(i)
            assert isinstance(dto, DTO)
            self.check_results(dto)

        result = dao.read(99)
        assert result is None

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
            assert dto.id == i

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

        dto = dao.read_by_name_mode(name="datasource_url_2", mode='test')
        assert isinstance(dto, DTO)
        self.check_results(dto)

        dto = dao.read_by_name_mode(name="datasource_url_1", mode='skdi')
        assert dto is None

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
    def test_update(self, container, datasources, caplog):
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
        dao.database.connection.begin()
        dtos = dao.read_all()
        for i, dto in dtos.items():
            dto.url = "www.rollback.com"
            dao.update(dto)

        cnx = dao.database.connection
        cnx.rollback()

        dtos = dao.read_all()
        for i, dto in dtos.items():
            assert not dto.url == "www.rollback.com"

        for i, dto in dtos.items():
            dto.url = "www.committed.com"
            dao.update(dto)

        db = dao.database
        db.save()

        dtos = dao.read_all()
        for i, dto in dtos.items():
            assert dto.url == "www.committed.com"

        with pytest.raises(mysql.connector.ProgrammingError):
            bogus_datasource_url = datasources[0].urls["datasource_url_1"]
            bogus_datasource_url.id = 7890
            dao.update(bogus_datasource_url)

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
        dao.database.begin()
        dao.delete(7)
        assert not dao.exists(7)

        cnx = dao.database.connection
        cnx.rollback()

        assert dao.exists(7)

        with pytest.raises(mysql.connector.ProgrammingError):
            dao.delete(8743)

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
