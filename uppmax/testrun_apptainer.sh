#!/bin/bash

CMD="test_run_all(max_cmds_to_run=0, max_workers=9, startup_stagger_sec=0.1)" # testrun_stage() r.testrun_all()
ID=15480 # full_qc=15164 #"15118" # "13744" 27feat=15171 15253/254 feat 15308=4.2.8 Cellpose 4

# add -s for interactive shell and skip executing command
./run_cellprofiler_apptainer_pelle.sh \
  -d "/cpp_work/input/$ID" \
  -o "/cpp_work/output/$ID" \
  -w 32 \
  -e 1 \
  -V "v4.2.8-cellpose4.0.7" \
  -c "PYTHONPATH=/cpp python3 -c 'import cp_uppmax_runner as r; r.${CMD}'" \
  -s

#      -c "PYTHONPATH=/cpp python3 -c 'import cp_uppmax_runner as r; r.testrun_stage()'"
#      -c "PYTHONPATH=/cpp python3 -c 'import cp_uppmax_runner as r; r.testrun_stage()'"
#      -c "PYTHONPATH=/cpp python3 -c 'import cp_uppmax_runner as r; r.testrun1()'"
#      -c "PYTHONPATH=/cpp python3 -c 'import cp_uppmax_runner as r; r.test_run_all()'"
