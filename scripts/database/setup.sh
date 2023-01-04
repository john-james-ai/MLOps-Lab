#!/usr/bin/env bash
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /setup/database/setup.sh                                                            #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday January 2nd 2023 10:03:05 pm                                                 #
# Modified   : Monday January 2nd 2023 11:36:37 pm                                                 #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2023 John James                                                                 #
# ================================================================================================ #
echo $'\nRestarting MySQL Server...'
sudo /etc/init.d/mysql restart

echo $'\nSetup Password...'
sudo mysql -u root -p --database mysql < setup/database/root_pwd.sql

echo $'\nRestart Database...'
sudo /etc/init.d/mysql restart

echo $'\nRunning secure installation...'
sudo mysql_secure_installation

echo $'\nStart Database...'
sudo /etc/init.d/mysql restart

echo $'\nCreate Database...'
sudo mysql -u root -p --database mysql < setup/database/create_db.sql

# echo $'\nSign in as new user...'
# sudo mysql -u root -p