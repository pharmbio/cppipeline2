#!/bin/bash

how_help() {
    echo "Usage: $0 -d INPUT_DIR -w MAX_WORKERS -e MAX_ERRORS"
    echo
    echo "Options:"
    echo "  -d    Specify the input directory (Mandatory)."
    echo "  -o    Specify the output directory (Mandatory)."
    echo "  -w    Set the maximum number of workers (Mandatory)."
    echo "  -e    Set the maximum number of errors (Mandatory)."
    echo "  -h    Show this help message."
}

# Parse command-line options
while getopts ":d:o:w:e:h" opt; do
  case $opt in
    d) INPUT_DIR="$OPTARG" ;;
    o) OUTPUT_DIR="$OPTARG" ;;
    w) MAX_WORKERS="$OPTARG" ;;
    e) MAX_ERRORS="$OPTARG" ;;
    h) show_help
       exit 0
       ;;
    \?) echo "Invalid option -$OPTARG" >&2
        show_help
        exit 1
        ;;
    :) echo "Option -$OPTARG requires an argument." >&2
       show_help
       exit 1
       ;;
  esac
done

# Check if mandatory options were provided
if [ -z "$INPUT_DIR" ] || [ -z "$MAX_WORKERS" ] || [ -z "$MAX_ERRORS" ]; then
    echo "Error: All options -d, -o, -w, and -e are mandatory." >&2
    show_help
    exit 1
fi

export INPUT_DIR
export OUTPUT_DIR
export MAX_WORKERS
export MAX_ERRORS
export OMP_NUM_THREADS=1

# Mount fileserver (resultdir and work-dir)
#./umount-dir.sh
#./mount-dir.sh

# load modules
ml PDC
ml singularity
ml htop

# singularity has changed name to apptainer
singularity exec \
          -H $PWD:/cpp \
          --bind /tmp:/tmp-stage \
          --bind /var/tmp:/tmp-stage2 \
          --bind /cfs/klemming/home/a/andlar5:/cfs/klemming/home/a/andlar5 \
          --bind /cfs/klemming/projects/supr/pb-data/stage/mikro:/share/mikro \
          --bind /cfs/klemming/projects/supr/pb-data/stage/mikro2:/share/mikro2 \
          --bind /cfs/klemming/projects/supr/pb-data/stage/external-datasets:/share/external-datasets \
          --bind /cfs/klemming/projects/supr/pb-data/cpp_work:/cpp_work \
          /cfs/klemming/projects/supr/pb-data/singularity/cpp_dardel_worker-v4.2.5-cellpose2.0-latest.sif \
          python3 /cpp/cp_dardel_runner.py

#./umount-dir.sh


