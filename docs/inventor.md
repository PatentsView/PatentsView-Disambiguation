# Inventor Disambiguation


## Overall Approach

We'll look at five key steps in disambiguation:

1. [Loading data](#Loading-data)
2. [Featurizing data](#Featurizing-data)
3. [Clustering data](#Clustering-data)
4. [Storing results](#Storing-results)
5. [Incremental additions](#Incremental-additions)

We will then review how these five steps are called
in a run of the disambiguation algorithm in the
[Putting it all Together](#Putting-it-all-Together) section.

### Loading data

The base data structure used in inventor disambiguation is
an [InventorMention](../pv/disambiguation/core.py)

```Python
from pv.disambiguation.core import InventorMention
```

We can build these objects from a row in the SQL table:

```Python
# methods for grabbing data from SQL
from pv.disambiguation.inventor.load_mysql import get_granted, get_pregrants

# connection to database
from pv.disambiguation.util.db import granted_table, pregranted_table

# database configuration
import configparser
config = configparser.ConfigParser()
config.read(['config/database_config.ini', 'config/database_tables.ini'])

# create a connection to the table
granted = granted_table(config)

# build inventor mentions
id_set = ['3essf45kxyx93pbr234ja64mc', '3wamwmh4wq8no3vberf1h320p', '48ho20qpwccw36wbiyblnu6wv', '6dtd8bga0klz896qjd4f8feqn', '9gwzhan13q4l7vj7y8d2if50h', 'esc86tobe7ureqzafay0b0fqq', 'pmxr0hekf36ujci0bwfuq74z4', 'rrv2yarv9uuc8x3arbbuwfamb', 'tz02w97mt6ag5expzbjfwqorw', 'wo8zrvcquvvs8i5vilbvv2hzo', 'zeaw2bhoonft48zi56sei89ho']
# internally this calls mentions = [InventorMention.from_granted_sql_record(r) for r in records]
mentions = get_granted(id_set, granted)
```

Each of these inventor mentions stores relevant info for disambiguation:

```Python
print(mentions[0].__dict__))
```

```
{'uuid': '3essf45kxyx93pbr234ja64mc',
 'patent_id': '8022252',
 'rawlocation_id': 'lhissxq7fdourrpl9zgswsddb',
 'raw_last': 'Ramaseshan',
 'raw_first': 'Mahesh',
 'sequence': '5',
 'rule_47': 'FALSE',
 'deceased': 'FALSE',
 'name': 'Mahesh Ramaseshan',
 'document_number': None,
 'mention_id': '8022252-5',
 'assignees': [],
 'title': None,
 'coinventors': [],
 '_first_name': ['mahesh'],
 '_first_initial': ['m'],
 '_first_letter': ['m'],
 '_middle_name': [],
 '_middle_initial': [],
 '_suffixes': [],
 '_last_name': ['ramaseshan'],
 'city': None,
 'state': None,
 'country': None,
 'record_id': '8022252'}
```

#### Canopies

Typically, we will be loading a batch of data corresponding to a canopy

```Python
from pv.disambiguation.inventor.load_mysql import Loader

# this object will load from two files of canopies:
# config['inventor']['pregranted_canopies'] = 'data/inventor/archive/canopies.pregranted.pkl'
# config['inventor']['granted_canopies'] = 'data/inventor/archive/canopies.granted.pkl'
loader = Loader.from_config(config, 'inventor')
```


We can load data for a particular canopy:

```Python
mentions = loader.load_canopies(['fl:m_ln:ramaseshan'])
```

We'll see how to create these files in the [Putting it all Together](#Putting-it-all-Together) section.


### Featurizing Data

In order to cluster the data, we will need to build a set of features
that will be used to compute the similarity between two records:

To do so,  we need to load the model which will provide
the features:

```Python
from pv.disambiguation.inventor.model import InventorModel
encoding_model = InventorModel.from_config(config)
```

We can inspect what the features are of the model:

```Python
print(list(map(encoding_model.feature_list),lambda x: x.name)))
```

Notice that the model uses these features:

```
['title', 'first_name', 'middle_initial', 'middle_name', 'suffix', 'canopy', 'coinventors', 'assignees']
```

Each feature has a name and information about how it is computed:

```Python
# (name, computation, how to aggregate feature in cluster, is must link constraint, is must not link constraint)
(first_name, FeatCalc.NO_MATCH, CentroidType.BINARY, False, True)
(title_model, FeatCalc.DOT, CentroidType.NORMED, False, False),
```

Given a set of records, we extract their features using:

```Python
features = encoding_model.encode(mentions)
```

The result is a list of tuples:

```Python
print(features)
```

```
[('title',
  True,
  700,
  array([[-0.02830344, -0.02209232, -0.06242473, ..., -0.02439391,
           0.04169574, -0.03287862],
         [-0.02812981, -0.02057877, -0.01793056, ..., -0.019576  ,
           0.02303531, -0.07892601],
         [-0.04294564, -0.05394144,  0.02167187, ..., -0.02340932,
          -0.02886444, -0.07576558],
         ...,
         [-0.0297003 , -0.0040316 , -0.04299174, ...,  0.00533789,
           0.01088064, -0.06962568],
         [-0.00942719,  0.01676086,  0.01141414, ..., -0.02976037,
          -0.04124262,  0.0052724 ],
         [-0.04569396, -0.03412756, -0.01593411, ..., -0.00681357,
           0.01439461, -0.04614205]], dtype=float32),
  <FeatCalc.DOT: 2>,
  <CentroidType.NORMED: 2>),
 ('first_name',
  True,
  1,
  array([[244674],
         [244674],
         [244674],
         [244674],
         [244674],
         [244674],
         [244674],
         [244674],
         [244674],
         [244674],
         [244674],
         [244674],
         [244674],
         [244674],
         [244674],
         [244674],
         [244674],
         [244674],
         [244674],
         [244674],
         [244674],
         [244674],
         [244674],
         [244674]], dtype=int32),
 ('coinventors',
  False,
  1048576,
  <24x1048576 sparse matrix of type '<class 'numpy.float32'>'
        with 173 stored elements in Compressed Sparse Row format>,
  <FeatCalc.DOT: 2>,
  <CentroidType.NORMED: 2>),
  ...]
```

These features will form the basis of our clustering model.
Each tuple in the list has the form
 `(name, is_dense_feature, dim, features (num mentions by dim), how to compute feature, how to compute aggregate)`
A couple of things to note about the features: title features
are vector embeddings from the Sent2Vec model, name features are hashes
of the names of the inventor, coinventor names are also hashed and
stored as sparse vectors in a scipy CSR matrix.

### Clustering model

To do the clustering, we will need to first define our
pairwise similarity model, which takes as input
sets of features:

```Python
model = torch.load(config['inventor']['model']).eval()
```

Run the clustering:

```Python
from grinch.agglom import Agglom
grinch = Agglom(model, features, num_points=len(mentions))
grinch.build_dendrogram_hac()
fc = grinch.flat_clustering(model.aux['threshold'])
```

`fc[i]` gives the cluster assignment of the `i`th record in `mentions`.

#### Partitioning the data

To run the clustering in a more parallelizable way,
we partition based on canopies/blocks.

Each canopy is defined by the first letter & last name of the inventor
e.g., `'fl:m_ln:ramaseshan'`

We do this by grouping the canopies into chunks:

```Python
# grab all the canopies in the dataset
all_canopies = set(loader.pregranted_canopies.keys()).union(set(loader.granted_canopies.keys()))

# singletons will be treated as their own chunk
singletons = set([x for x in all_canopies if loader.num_records(x) == 1])

# we sort so that we roughly evenly partition the data into equal sized chunks
all_canopies_sorted = sorted(list(all_canopies.difference(singletons)), key=lambda x: (loader.num_records(x), x),
                             reverse=True)


# chunk all of the data by canopy
chunks = [[] for _ in range(num_chunks)]
for idx, c in enumerate(all_canopies_sorted):
    chunks[idx % num_chunks].append(c)
```

That is done at the time that `run_clustering.py` is called.
It will write these chunks to a file:

```Python
logging.info('Saving chunk to canopy map')
with open(outdir + '/chunk2canopies.pkl', 'wb') as fout:
    pickle.dump([chunks, list(singletons)], fout)
```

`chunks` is a list of lists, `chunks[i]` gives the canopies in the ith
chunk. `singletons` are all the canopies with just a single mention.
Canopies for inventors are given by strings such as `'fl:m_ln:ramaseshan'`.


### Storage of Results

The results of disambiguation will be stored in two ways. The first
will be the results to populate the resulting database,
the second will support incremental additions.


First we will store the flat clustering results. These will
be in a dictionary `canopy2predictions` updated by

```Python
# all_pids is a list of UUIDs for the mentions
# all_canopies is a list of the canopies for the mentions (typically will be all the same canopy)
canopy2predictions[all_canopies[i]][0].append(all_pids[i])
canopy2predictions[all_canopies[i]][1].append('%s-%s' % (all_canopies[i], fc[i]))

# these will be saved in pickle files:
job_name='chunk-%s' % chunk_number
results = canopy2predictions
outfile = os.path.join(outdir, job_name) + '.pkl'
with open(outfile, 'wb') as fin:
    pickle.dump(results, fin)
```

Next, we will store the resulting tree structures
for the sake of incremental additions.
```Python
# trees is a list of all the trees
tree_id = len(trees)
# store the tree that is build for this canopy
trees.append(grinch)
for i in range(len(all_pids)):
    # record mapping from canopy to the tree id
    canopy2tree_id[all_canopies[i]] = tree_id

# we need to use torch to save this pickle file because we store
# the inventor model as a torch object
grinch_trees = []
for idx,t in tqdm(enumerate(tree_list), total=len(tree_list)):
    grinch = WeightedMultiFeatureGrinch.from_agglom(t, pids_list[idx])
    grinch.prepare_for_save()
    grinch_trees.append(grinch)
torch.save([grinch_trees, canopy2tree_id], outstatefile)
```

There will be one file for each chunk.

### Incremental Additions

Performing an incremental addition involves:
(1) loading the tree structure into memory,
(2) inserting new mentions
(3) updating predicted entities

This looks like:

```Python

# Load all the trees for a chunk of the data
[grinch_trees, canopy2tree_id] = torch.load(statefile)
statefile = os.path.join(outdir, 'job-%s' % this_chunk_id) + 'internals.pkl'
grinch = grinch_trees[canopy2tree_id[c]]

# Featurize new data
all_records = loader.load_canopies([c])
features = encoding_model.encode(all_records)

# Insert the new data
grinch.perform_graft = False
grinch.update_and_insert(features, all_pids)

# Grab predicted entities
fc = grinch.flat_clustering(weight_model.aux['threshold'])
```

## Putting it all Together

### Canopies & Featurizers

Each of these scripts can be run in parallel:

```bash
python -m pv.disambiguation.inventor.build_assignee_features_sql
python -m pv.disambiguation.inventor.build_coinventor_features_sql
python -m pv.disambiguation.inventor.build_title_map_sql
python -m pv.disambiguation.inventor.build_canopies_sql
```

Each will output a dictionary stored in pickle file(s)

The canopies script wil store:

```
canopies2uuid[canopy_name] = [list_of_uuids]
e.g.
canopies2uuid['fl:m_ln:ramaseshan'] = ['3essf45kxyx93pbr234ja64mc', '3wamwmh4wq8no3vberf1h320p', '48ho20qpwccw36wbiyblnu6wv', '6dtd8bga0klz896qjd4f8feqn', '9gwzhan13q4l7vj7y8d2if50h', 'esc86tobe7ureqzafay0b0fqq', 'pmxr0hekf36ujci0bwfuq74z4', 'rrv2yarv9uuc8x3arbbuwfamb', 'tz02w97mt6ag5expzbjfwqorw', 'wo8zrvcquvvs8i5vilbvv2hzo', 'zeaw2bhoonft48zi56sei89ho']
```

The output of the assignee features is:

```bash
features[patent_id] = [list_of_assignee_names]
e.g.
features['8022252'] = ['Tranzyme Pharma Inc.']
```

The output of the title map is:

```bash
features[patent_id] = patent_title
e.g.
features['pg-20180280413'] = Compositions and Methods For Increasing Telomerase Activity]
```

Note that the title features will be vectors.

The output of the co-inventor map is:

```bash
features['pg-20050119169] = ['deslongchamps', 'dory', 'ouellet', 'villeneuve', 'ramaseshan', 'fortin', 'peterson', 'hoveyda', 'beaubien', 'marsault']
```

### Running Full Batch Clustering

```
python -m pv.disambiguation.inventor.run_clustering
```

We will run this command `num_chunks+1` times (one for each chunk,
once for the chunk of singletons). We will do so by updating in
the config file which chunk is to be executed:

```
$ vi config/inventor/run_clustering.ini
chunk_id = singletons

$ vi config/inventor/run_clustering.ini
chunk_id = 0

$ vi config/inventor/run_clustering.ini
chunk_id = 1

etc.
```

Of course this can be done programmatically.
Also, each chunk can be run independently in
parallel.


### Running Incremental Updates

To perform an incremental update,
we will assume that there is a
new database table that we would like to run
the disambiguation on.

We will update the corresponding database
config files to point to those tables:

```
$ vi config/database_tables.ini

[DATABASE]

granted_patent_database =
pregrant_database =
```

We will update each of the other config files with
new filenames for the incremental approach: e.g.,
```
[INVENTOR_BUILD_ASSIGNEE_FEAT]

feature_out = data/inventor/update_2021_04_11/assignee_features
```

We then re-run:

```bash
python -m pv.disambiguation.inventor.build_assignee_features_sql
python -m pv.disambiguation.inventor.build_coinventor_features_sql
python -m pv.disambiguation.inventor.build_title_map_sql
python -m pv.disambiguation.inventor.build_canopies_sql
```

Now, we can run:

```python
python -m pv.disambiguation.inventor.incremental_update
```