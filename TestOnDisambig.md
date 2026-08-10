## TEST ON REMOTE ENV

1. ssh PVDis
2. `conda activate disambig`
3. `export PYTHONPATH="/home/centos/ba_disambiguation_20230330:$PYTHONPATH"`
4. `export DISAMBIGUATION_ROOT=/home/centos/ba_disambiguation_20230330`
5. ``python assignee_clustering_runner.py``