#!/bin/sh

cd `dirname $0`
hadoop dfs -rm -rf /data/mr-correlation/out
hadoop dfs -put ./data/in/sales.csv /data/mr-correlation/in
hadoop jar build/libs/mr-correlation.jar org.myorg.mr.average.AverageJob  /data/mr-correlation/in /data/mr-correlation/out

