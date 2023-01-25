#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Recommender Systems: Towards Deep Learning State-of-the-Art                         #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.6                                                                              #
# Filename   : /scripts/frameworks/pachyderm.sh                                                    #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/Recommender-Systems                                #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday January 24th 2023 07:18:48 pm                                               #
# Modified   : Tuesday January 24th 2023 07:49:21 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2023 John James                                                                 #
# ================================================================================================ #
# Install Homebrew
echo "Install Homebrew using Debian Installation"
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
# Add Homebrew to PATH
echo '# Set PATH, MANPATH, etc., for Homebrew.' >> /home/john/.profile
echo 'eval "$(/home/linuxbrew/.linuxbrew/bin/brew shellenv)"' >> /home/john/.profile
eval "$(/home/linuxbrew/.linuxbrew/bin/brew shellenv)"
# Install Homebrew Dependencies
echo "Install Homebrew Dependencies"
sudo apt-get install build-essential
echo "Install GCC"
brew install gcc
# Install Pachctl CLI
echo "Install Pachctl CLI"
curl -o /tmp/pachctl.deb -L https://github.com/pachyderm/pachyderm/releases/download/v2.4.4/pachctl_2.4.4_amd64.deb && sudo dpkg -i /tmp/pachctl.deb
# Install and Configure Helm
echo "Install Helm"
brew install helm
# Add Pachyderm repo to Helm
echo "Add th  Pachyderm repo to Helm"
helm repo add pachyderm https://helm.pachyderm.com
helm repo update
# Install PachD
helm install pachyderm pachyderm/pachyderm --set deployTarget=LOCAL --set proxy.enabled=true --set proxy.service.type=LoadBalancer
# Connect to Cluster
echo '{"pachd_address":"grpc://127.0.0.1:80"}' | pachctl config set context local --overwrite && pachctl config set active-context local