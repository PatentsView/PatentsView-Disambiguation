# Inventor Disambiguation


## Overall Approach

We'll look at five key steps in disambiguation:

1. Loading data
2. Featurizing data
3. Clustering data
4. Storing results
5. Incremental additions

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
we partition based on canopies.

We do this by grouping the canopies into chunks:

```Python

```

That is done at the time that `run_clustering.py` is called.
It will write these chunks to a file:

```Python

```

The type of this data is:

```Python

```


### Storage of Results

We store the results of the clustering in two ways:

First we will store the flat clustering results:

```Python

```

Then we will store the tree structures built:

```Python

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

```

The output of the canopy creation is:

```bash

```

The output of the assignee features is:

```bash

```

The output of the title map is:

```bash

```

Note that the title features will be vectors.

```

```

### Running Full Batch Clustering

```

```

### Running Incremental Updates

```

```