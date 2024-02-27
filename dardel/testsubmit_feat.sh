#!/bin/bash
           sbatch \
	   --nodes 1 \
           -p main \
           --mem 220GB \
	   -t 14:00:00 \
	   --output=logs/xxx-slurm.%j.out \
           --error=logs/xxx-slurm.%j.out \
	   -A naiss2023-22-1320 \
	   -D /cfs/klemming/home/a/andlar5/cpp \
           run_cellprofiler_singularity_dardel.sh \
             -d /cpp_work/input/8570 \
             -o /cpp_work/output/8570 \
             -w 50 \
             -e 10
