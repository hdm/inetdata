#!/bin/bash

if [ -f /etc/profile.d/rvm.sh  ]; then
  source /etc/profile.d/rvm.sh
fi

export PATH=/usr/local/bin:$PATH
export GODEBUG=cgocheck=0
THIS=$(readlink -f "$(dirname "$(readlink -f "$0")")/")
exec ${THIS}/normalize.rb $@
