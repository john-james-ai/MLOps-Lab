#!/usr/bin/env bash
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /setup/database/build.sh                                                            #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday January 2nd 2023 10:03:05 pm                                                 #
# Modified   : Monday January 2nd 2023 11:36:54 pm                                                 #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2023 John James                                                                 #
# ================================================================================================ #
echo $'\nStop MySQL processes..'
sudo /etc/init.d/mysql stop

echo $'\nRemoving existing installation..'
sudo apt purge mysql-server mysql-client mysql-common mysql-server-core-* mysql-client-core-* -y
sudo apt-get remove mysql-* -y
sudo apt-get purge mysql-* -y
sudo apt remove dbconfig-mysql -y

echo $'\nDelete all MySQL files...'
sudo rm -rf /etc/mysql /var/lib/mysql /var/log/mysql

echo $'\nCleaning packages not needed...'
sudo apt-get autoclean -y
sudo apt autoremove -y
sudo apt autoclean -y
# Follow instructions at https://docs.microsoft.com/en-us/windows/wsl/tutorials/wsl-database
# use wsl terminal

echo $'\nSetting Home Directory...'
sudo usermod -d /var/lib/mysql/ mysql

echo $'\nUpdating distribution...'
sudo apt-get dist-upgrade -y

echo $'\nUpdating packages...'
sudo apt update -y

echo $'\nInstalling MySQL Server...'
sudo apt-get install mysql-server -y

echo $'\nStarting MySQL Server...'
sudo /etc/init.d/mysql start

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