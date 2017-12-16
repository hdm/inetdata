#!/bin/bash

if [ -f /etc/profile.d/rvm.sh  ]; then
  source /etc/profile.d/rvm.sh
fi

if [ -f ${HOME}/.bashrc ]; then
  source ${HOME}/.bashrc
fi

THIS=$(readlink -f "$(dirname "$(readlink -f "$0")")/")
exec ${THIS}/download.rb $@
