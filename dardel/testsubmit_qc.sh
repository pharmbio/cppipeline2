#!/bin/bash
sbatch \
	  --nodes 1 \
    -p main \
    --mem 440GB \
	  -t 00:10:00 \
	  --output=logs/xxx-slurm.%j.out \
    --error=logs/xxx-slurm.%j.out \
	  -A naiss2023-22-1320 \
	  -D /cfs/klemming/home/a/andlar5/cpp \
    run_cellprofiler_singularity_dardel.sh \
      -d /cpp_work/input/8550 \
      -o /tmp/stage/output/8550 \
      -w 50 \
      -e 10
