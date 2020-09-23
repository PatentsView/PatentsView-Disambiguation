# PatentsView-Disambiguation

## Setup

```
pip install git+git://github.com/iesl/grinch.git
conda install pytorch==1.2.0 torchvision==0.4.0 cudatoolkit=9.2 -c pytorch 
```

## Inventor

### Build Features

```
python -m pv.disambiguation.inventor.build_assignee_features_sql.py
python -m pv.disambiguation.inventor.build_coinventor_features_sql.py
python -m pv.disambiguation.inventor.build_title_map_sql.py
```

### Build Canopies

```
python -m pv.disambiguation.inventor.build_canopies_sql.py --source granted
python -m pv.disambiguation.inventor.build_canopies_sql.py --source pregranted
```

### Run clustering

```
wandb sweep bin/inventor/run_all.yaml
wandb agent $sweep_id
# or using slurm
sh bin/launch_sweep.sh $sweep_id
```

### Finalize

```
python -m pv.disambiguation.inventor.finalize
```

### Optional: Train Your Own Model

This command trains an inventor disambiguation model.

```
python -m pv.disambiguation.inventor.train_model.py --training_data data/inventor-train/eval_common_characteristics.train --dev_data data/inventor-train/eval_common_characteristics.dev --max_num_dev_canopies 200
```

## Assignee

### Build Mentions & Canopies

```
python -m pv.disambiguation.assignee.build_assignee_name_mentions_sql.py
```

### Run clustering

```
wandb sweep bin/assignee/run_all.yaml
wandb agent $sweep_id
# or using slurm
sh bin/launch_sweep.sh $sweep_id
```

### Finalize

```
python -m pv.disambiguation.assignee.finalize
```

## Location

### Build Mentions & Canopies

```
python -m pv.disambiguation.location.build_assignee_location_canopies.py
python -m pv.disambiguation.location.build_inventor_location_canopies.py
python -m pv.disambiguation.location.build_assignee_location_mentions.py
python -m pv.disambiguation.location.build_inventor_location_mentions.py
```

### Run clustering

```
wandb sweep bin/location/run_all.yaml
wandb agent $sweep_id
# or using slurm
sh bin/launch_sweep.sh $sweep_id
```

### Finalize

```
python -m pv.disambiguation.location.finalize
```