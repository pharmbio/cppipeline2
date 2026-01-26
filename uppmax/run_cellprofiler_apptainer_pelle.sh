#!/bin/bash
set -euo pipefail

show_help() {
    cat <<EOF
Usage: $0 -d INPUT_DIR -o OUTPUT_DIR -w MAX_WORKERS -e MAX_ERRORS [ -c "CMD" ] [ -V VERSION ]

Mandatory options
  -d  INPUT_DIR        path passed to the container (exported as \$INPUT_DIR)
  -o  OUTPUT_DIR       path passed to the container (exported as \$OUTPUT_DIR)
  -w  MAX_WORKERS      thread-/process-pool size
  -e  MAX_ERRORS       fail fast after this many CP job errors

Optional
  -c  "CMD"            command to run inside the container
                       (default: python3 /cpp/cp_uppmax_runner.py)
  -V  VERSION          container version tag (maps to /proj/cellprofiling/nobackup/cpp2_uppmax_worker-<VERSION>-latest.sif; default: v4.2.8-cellpose4.0.7)
  -s                   open an interactive shell instead of running the command
  -h  this help
EOF
}

# --- defaults --------------------------------------------------------------
RUN_CMD='python3 /cpp/cp_uppmax_runner.py'
CONTAINER_VERSION='v4.2.5-cellpose2.0'
USE_SHELL=false

# --- parse options ---------------------------------------------------------
while getopts ":sd:o:w:e:c:V:h" opt; do
  case $opt in
    s)
      USE_SHELL=true
      next_arg="${!OPTIND:-}"
      if [[ -n $next_arg && $next_arg != -* ]]; then
          echo "Warning: -s takes no argument; ignoring '$next_arg'." >&2
          ((OPTIND++))
      fi
      ;;
    d) INPUT_DIR="$OPTARG"      ;;
    o) OUTPUT_DIR="$OPTARG"     ;;
    w) MAX_WORKERS="$OPTARG"    ;;
    e) MAX_ERRORS="$OPTARG"     ;;
    c) RUN_CMD="$OPTARG"        ;;
    V) CONTAINER_VERSION="$OPTARG" ;;
    h) show_help; exit 0        ;;
    \?) echo "Invalid option: -$OPTARG" >&2; show_help; exit 1 ;;
    :)  echo "Option -$OPTARG requires an argument." >&2; show_help; exit 1 ;;
  esac
done

# --- sanity check ----------------------------------------------------------
if [[ -z ${INPUT_DIR:-} || -z ${OUTPUT_DIR:-} || -z ${MAX_WORKERS:-} || -z ${MAX_ERRORS:-} ]]; then
    echo "Error: -d, -o, -w and -e are mandatory." >&2
    show_help; exit 1
fi

# --- export env vars for the runner ----------------------------------------
export INPUT_DIR OUTPUT_DIR MAX_WORKERS MAX_ERRORS
export OMP_NUM_THREADS=1
export CELLPOSE_LOCAL_MODELS_PATH="/root/.cellpose/models/"

# --- Apptainer -------------------------------------------------------------
STAGE_ROOT="$TMPDIR/pharmbio/stage"
CPP_WORK="$TMPDIR/pharmbio/cpp_work"
CPP_ROOT="$(pwd)"
CONTAINER_IMAGE_PATH="/proj/cellprofiling/nobackup/cpp2_uppmax_worker-$CONTAINER_VERSION-latest.sif"

mkdir -p "$STAGE_ROOT/mikro"
mkdir -p "$STAGE_ROOT/mikro2"
mkdir -p "$STAGE_ROOT/mikro3"
mkdir -p "$STAGE_ROOT/mikro4"
mkdir -p "$STAGE_ROOT/external-datasets"
mkdir -p "$CPP_WORK/input"

if "$USE_SHELL"; then
    apptainer shell \
        --bind $CPP_ROOT:/cpp \
        --bind $STAGE_ROOT/mikro:/share/mikro \
        --bind $STAGE_ROOT/mikro2:/share/mikro2 \
        --bind $STAGE_ROOT/mikro3:/share/mikro3 \
        --bind $STAGE_ROOT/mikro4:/share/mikro4 \
        --bind $STAGE_ROOT/external-datasets:/share/data/external-datasets \
        --bind $CPP_WORK:/cpp_work \
        "$CONTAINER_IMAGE_PATH"
else
    apptainer exec \
        --bind $CPP_ROOT:/cpp \
        --bind $STAGE_ROOT/mikro:/share/mikro \
        --bind $STAGE_ROOT/mikro2:/share/mikro2 \
        --bind $STAGE_ROOT/mikro3:/share/mikro3 \
        --bind $STAGE_ROOT/mikro4:/share/mikro4 \
        --bind $STAGE_ROOT/external-datasets:/share/data/external-datasets \
        --bind $CPP_WORK:/cpp_work \
        "$CONTAINER_IMAGE_PATH" \
        sh -c "$RUN_CMD"
fi
