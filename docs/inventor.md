# Inventor Disambiguation


## Overall Approach

We'll look at five key steps in disambiguation:

1. Loading data
2. Featurizing data
3. Clustering data
4. Storing results
5. Incremental additions

We will then put this all together with pointers in the code
for how this is run.

### Loading data

The base data structure used in inventor disambiguation is
an [InventorMention](pv/disambiguation/core.py)

```Python
from pv.disambiguation.core import InventorMention
```

We can build these objects from a row in the SQL table:

```Python
# methods for grabbing data from SQL
from pv.disambiguation.inventor.load_mysql import get_granted, get_pregranted

# connection to database
from pv.disambiguation.util.db import granted_table, pregranted_table

# database configuration
import configparser
config = configparser.ConfigParser()
config.read(['config/database_config.ini', 'config/database_tables.ini'])

# create a connection to the table
granted = granted_table(config)

# build inventor mentions
id_set = []
# internally this calls mentions = [InventorMention.from_granted_sql_record(r) for r in records]
mentions = get_granted(id_set, granted)
```

Each of these inventor mentions stores relevant info for disambiguation:

```Python
print(mentions[0].__dict__))
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
mentions = loader.load_canopies([''])
```

We'll see how to create these files in the Putting It All Together section.


### Featurizing Data

In order to cluster the data, we will need to build a set of features
that will be used to compute the similarity between two records:

To do so,  we need to load the model which will provide
the features:

```Python
encoding_model = InventorModel.from_config(config)
```

We can inspect what the features are of the model:

```Python
print(list(map(encoding_model.feature_list),lambda x: x.name)))
```

Notice that the model uses these features:

```

```

Each feature will have a name and information about how it is computed:

```Python
(first_name, FeatCalc.NO_MATCH, CentroidType.BINARY, False, True)
```

```Python
(title_model, FeatCalc.DOT, CentroidType.NORMED, False, False)
```

Given a set of records, we extract their features using:

```Python
features = encoding_model.encode(mentions)
```

The result is a list of tuples:

```Python
print(features)
```

These features will form the basis of our clustering model.

### Clustering model

To do the clustering, we will need to first define our
pairwise similarity model, which takes as input
sets of features:

```Python
model = torch.load(config['inventor']['model']).eval()
```

Run the clustering:

```Python
grinch = Agglom(model, features, num_points=len(mentions))
grinch.build_dendrogram_hac()
fc = grinch.flat_clustering(model.aux['threshold'])
```


Want to know more about what goes on inside this code?

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

## Putting it all together

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