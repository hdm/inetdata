#!/bin/bash

THIS=$(readlink -f "$(dirname "$(readlink -f "$0")")/")
${THIS}/download.sh && ${THIS}/normalize.sh

