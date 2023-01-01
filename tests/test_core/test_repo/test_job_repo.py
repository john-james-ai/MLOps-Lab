#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /tests/test_core/test_repo/test_job_repo.py                                         #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday December 31st 2022 11:58:21 pm                                             #
# Modified   : Sunday January 1st 2023 03:15:39 am                                                 #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
import inspect
from datetime import datetime
import pytest
import logging

from recsys.core.entity.job import Job, Task

# ------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #


@pytest.mark.repo
@pytest.mark.job_repo
class TestJobRepo:  # pragma: no cover
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
        table = container.table.job()
        table.reset()
        table = container.table.task()
        table.reset()
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
    def test_add_get(self, container, jobs, caplog):
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
        repo = container.repos.job()
        for i, job in enumerate(jobs, start=1):
            job = repo.add(job)
            assert job.id == i
            j2 = repo.get(i)
            assert job == j2
            assert len(job) == len(j2)
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
    def test_get_by_name(self, container, caplog):
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
        repo = container.repos.job()
        job = repo.get_by_name_mode(name="job_name_2")
        assert isinstance(job, Job)
        for name, task in job.tasks.items():
            assert isinstance(task, Task)
            assert task.name == name
            assert task.parent == job
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
    def test_get_all(self, container, caplog):
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
        repo = container.repos.job()
        jobs = repo.get_all()
        logger.debug(jobs)
        assert isinstance(jobs, dict)
        for i, job in jobs.items():
            assert isinstance(job, Job)
            assert job.id == i
            assert job.oid is not None
            logger.debug(job.tasks)
            for j, task in job.tasks.items():
                assert isinstance(task, Task)
                assert task.name == j
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
    def test_update(self, container, caplog):
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
        repo = container.repos.job()
        jobs = repo.get_all()
        for i, job in jobs.items():
            job.state = "COMPLETE"
            for j, task in job.tasks.items():
                task.state = "COMPLETE"
                job.update_task(task)
            repo.update(job)

        jobs = repo.get_all()
        for i, job in jobs.items():
            assert job.state == "COMPLETE"
            for j, task in job.tasks.items():
                assert task.state == "COMPLETE"

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
    def test_remove_exists(self, container, caplog):
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
        repo = container.repos.job()
        for i in range(1, 6):
            if i % 2 == 0:
                repo.remove(i)
        assert len(repo) < 5

        for i in range(1, 6):
            if i % 2 == 0:
                with pytest.raises(FileNotFoundError):
                    repo.get(i)
            else:
                assert repo.exists(i)
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
    def test_print(self, container, caplog):
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
        repo = container.repos.job()
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
