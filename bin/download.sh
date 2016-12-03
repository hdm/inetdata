#!/bin/bash

if [ -f /etc/profile.d/rvm.sh  ]; then
  source /etc/profile.d/rvm.sh
fi

THIS=$(readlink -f "$(dirname "$(readlink -f "$0")")/")
exec ${THIS}/download.rb $@
