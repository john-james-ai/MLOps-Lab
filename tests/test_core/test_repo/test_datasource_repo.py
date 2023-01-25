#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Enter Project Name in Workspace Settings                                            #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /tests/test_core/test_repo/test_datasource_repo.py                                  #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : Enter URL in Workspace Settings                                                     #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday December 31st 2022 11:58:21 pm                                             #
# Modified   : Tuesday January 24th 2023 08:13:51 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import inspect
from datetime import datetime
import pytest
import logging

from mlops_lab.core.entity.datasource import DataSource, DataSourceURL
from mlops_lab.core.repo.datasource import DataSourceRepo

# ------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #
double_line = f"\n{100 * '='}"
single_line = f"\n{100 * '-'}\n"


@pytest.mark.repo
@pytest.mark.datasource_repo
class TestDataSourceRepo:  # pragma: no cover
    def reset_db(self, container) -> None:
        dba = container.dba.datasource()
        dba.reset()
        dba = container.dba.datasource_url()
        dba.reset()
        dba = container.dba.object()
        dba.reset()

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
        self.reset_db(container)
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
    def test_xaction_insert_no_commit(self, container, context, datasources, caplog):
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
        repo = DataSourceRepo(context)
        context.begin()
        for f1 in datasources.copy():
            f2 = repo.add(f1)
            assert repo.exists(f2.id)
        context.close()

        for i in range(1, 6):
            assert not repo.exists(i)

        self.reset_db(container)
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
    def test_xaction_insert_commit(self, container, context, datasources, caplog):
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
        repo = DataSourceRepo(context)
        context.begin()
        ids = []
        for f3 in datasources.copy():
            f4 = repo.add(f3)
            assert repo.exists(f4.id)
            ids.append(f4.id)
        context.save()
        context.close()

        for i in ids:
            assert repo.exists(i)
        self.reset_db(container)
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
    def test_xaction_update(self, container, context, datasources, caplog):
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
        repo = DataSourceRepo(context)

        for datasource in datasources:
            repo.add(datasource)

        context.begin()
        for i, datasource in enumerate(datasources, start=1):
            f1 = repo.get(i)
            f1.website = f"www.website_{99 + i}.com"
            repo.update(f1)
            f2 = repo.get(i)
            assert f2.website == f"www.website_{99 + i}.com"
        context.close()

        for i in range(1, 6):
            f3 = repo.get(i)
            assert not f3.website == f"www.website_{99 + i}.com"

        context.begin()
        for i in range(1, 6):
            f4 = repo.get(i)
            f4.website = f"www.website_{99 + i}.com"
            repo.update(f4)
            f5 = repo.get(i)
            assert f5.website == f"www.website_{99 + i}.com"
        context.save()
        context.close()

        for i in range(1, 6):
            f6 = repo.get(i)
            assert f6.website == f"www.website_{99 + i}.com"

        self.reset_db(container)
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
    def test_xaction_delete(self, container, context, datasources, caplog):
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
        repo = DataSourceRepo(context)

        for datasource in datasources:
            repo.add(datasource)

        context.begin()
        for i in range(1, 6):
            repo.remove(i)
            assert not repo.exists(i)
        context.rollback()
        context.close()

        for i in range(1, 6):
            assert repo.exists(i)

        context.begin()
        for i in range(1, 6):
            repo.remove(i)
            assert not repo.exists(i)
        context.save()
        context.close()

        for i in range(1, 6):
            assert not repo.exists(i)

        self.reset_db(container)
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
    def test_add_get(self, context, container, datasources, caplog):
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
        repo = DataSourceRepo(context)
        for i, datasource in enumerate(datasources, start=1):
            datasource = repo.add(datasource)
            assert datasource.id == i
            f2 = repo.get(i)
            assert datasource == f2
            for df in f2.urls.values():
                assert isinstance(df, DataSourceURL)
                assert "url" in df.url
                assert df.parent == f2

        self.reset_db(container)
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
    def test_get_by_name(self, container, datasources, context, caplog):
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
        repo = DataSourceRepo(context)

        for datasource in datasources:
            repo.add(datasource)

        datasource = repo.get_by_name(name="tenrec_2")
        assert isinstance(datasource, DataSource)
        assert datasource.id == 2
        assert datasource.name == "tenrec_2"
        for df in datasource.urls.values():
            assert isinstance(df, DataSourceURL)
            assert df.parent == datasource

        self.reset_db(container)
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
    def test_update(self, container, context, datasources, caplog):
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
        repo = DataSourceRepo(context)

        for datasource in datasources:
            repo.add(datasource)

        for i in range(1, 6):
            datasource = repo.get(i)
            datasource.website = f"www.website_{99 + i}.com"
            repo.update(datasource)

        for i in range(1, 6):
            datasource = repo.get(i)
            assert datasource.website == f"www.website_{99 + i}.com"

        self.reset_db(container)
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
    def test_remove_exists(self, container, datasources, context, caplog):
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
        repo = DataSourceRepo(context)

        for datasource in datasources:
            repo.add(datasource)

        for i in range(1, 6):
            if i % 2 == 0:
                repo.remove(i)
        assert len(repo) < 5

        for i in range(1, 6):
            if i % 2 == 0:
                assert repo.get(i) == []
                assert repo.exists(i) is False
            else:
                assert repo.exists(i)

        self.reset_db(container)
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
    def test_print(self, context, datasources, caplog):
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
        repo = DataSourceRepo(context)

        for datasource in datasources:
            repo.add(datasource)

        repo.print()
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
