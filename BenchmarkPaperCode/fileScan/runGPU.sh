#!/bin/bash
if [[ "$1" != "" && "$1" -le 8 ]]; then
    NGPU=$1
else
    echo "Defaulting to 1 GPU."
    NGPU=1
fi
TIME=$((270-30*$NGPU))
HOURS=$(($TIME/60))
MINUTES=$(($TIME%60))
CORES_PER_GPU=16
CORES=$(($CORES_PER_GPU * $NGPU))
printf "Requesting %d GPUs (+ %d CPU-cores) for %d minutes (%02d:%02d:00)\n\n" $NGPU $CORES $TIME $HOURS $MINUTES
srun -N 1 -n 1 -c $CORES --gres=gpu:a100:$NGPU --qos=devel -p dgx -t $TIME --pty bash


./spark-shell        --master local        --num-executors 12        --conf spark.executor.cores=4        --conf spark.rapids.sql.concurrentGpuTasks=4        --driver-memory 10g        --conf spark.rapids.memory.pinnedPool.size=8G        --conf spark.sql.files.maxPartitionBytes=512m        --conf spark.plugins=com.nvidia.spark.SQLPlugin



#val orcFilePath = "/scratch/hpc-prf-haqc/haikai/40M.orc"
#// Read ORC file
#val df = spark.read.format("orc").load(orcFilePath)
