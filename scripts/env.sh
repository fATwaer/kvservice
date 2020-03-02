#!/usr/bin/env bash

KVPATH=~/go/src/kvservice
WEBPATH=/data/menu/kv

mkdir -p ${WEBPATH}
cp ${KVPATH}/scripts/*.html ${WEBPATH}
