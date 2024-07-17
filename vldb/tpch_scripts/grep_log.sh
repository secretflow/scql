#!/bin/bash
path=${1}
time=${2}
echo ${path}

cat ${path}/sciengine.log | grep "running_cost" >${path}/running_cost.txt

# last time is start time
python3 handle.py ${path}/running_cost.txt ${time}

cat ${path}/sciengine.log | grep "start to execute node" >${path}/ant_op_extract.txt
cat ${path}/sciengine.log | grep "finished executing node" >${path}/ant_op_extract_end.txt
echo "${path}"
# last time is start time
python3 get_op.py ${path}/ant_op_extract.txt ${path}/ant_op_extract_end.txt ${time}