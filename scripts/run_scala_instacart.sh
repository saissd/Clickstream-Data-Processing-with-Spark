#!/usr/bin/env bash
set -euo pipefail
pushd spark_scala >/dev/null
sbt "runMain wiland.InstacartETL ../data ../data/gold_scala"
popd >/dev/null
