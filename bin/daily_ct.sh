#!/bin/bash

THIS=$(readlink -f "$(dirname "$(readlink -f "$0")")/")
${THIS}/download.sh -s ct && ${THIS}/normalize.sh -s ct

