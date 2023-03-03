[]()# Assignee Disambiguation

## Overall Approach

We'll look at four key steps in disambiguation:

1. [Loading data](#Loading-data)
2. [Featurizing data](#Featurizing-data)
3. [Clustering data](#Clustering-data)
4. [Storing results](#Storing-results)

We will then review how these steps are called
in a run of the disambiguation algorithm in the
[Putting it all Together](#Putting-it-all-Together) section.

### Loading data & Canopies

There are two kinds of mentions that we will be using:

- [AssigneeMention](../pv/disambiguation/core.py#L174) - Corresponds to one row in the rawassignee table
- [AssigneeNameMention](../pv/disambiguation/core.py#L312) - Corresponds to one unique string spelling (using the relaxed string match) of an assignee

Recall that we assume that if two assignee names have identical spelling that the two are the same assignee.

We load the `AssigneeNameMention`s from a precomputed pickle file of data.

```Python
from pv.disambiguation.core import AssigneeNameMention
```

We load these mentions by:

```Python
from pv.disambiguation.assignee.load_name_mentions import Loader

# configuration
import configparser
config = configparser.ConfigParser()
config.read(['config/database_config.ini', 'config/database_tables.ini', 'config/assignee/run_clustering.ini'])

loader = Loader.from_config(config)

mentions = loader.load('musi')
```

Each of these assignee name mentions stores relevant info for disambiguation:

```Python
print(mentions[0].__dict__))
```

```
{'uuid': '25fa40c1-16f3-4a6c-96e1-51dcf8a58dc6',
 'name_hash': 'universalmusic',
 'name_features': {'universal music', 'universalmusic'},
 'mention_ids': {'6918059-0',
  '7209892-0',
  '7376581-0',
  '7624046-0',
  '8204755-0',
  '8457977-0',
  '9230552-0',
  'pg-20050192871-1',
  'pg-20060184960-1',
  'pg-20080215776-1',
  'pg-20080222519-1',
  'pg-20100299151-1',
  'pg-20120253827-1',
  'pg-20130294606-1'},
 'record_ids': {'6918059',
  '7209892',
  '7376581',
  '7624046',
  '8204755',
  '8457977',
  '9230552',
  'pg-20050192871',
  'pg-20060184960',
  'pg-20080215776',
  'pg-20080222519',
  'pg-20100299151',
  'pg-20120253827',
  'pg-20130294606'},
 'location_strings': [],
 'canopies': {'musi', 'rsal', 'univ', 'usic'},
 'unique_exact_strings': {'Universal Music Group, Inc.': 12,
  'UNIVERSAL MUSIC GROUP, INC.': 1,
  'Universal Music Group': 1},
 'normalized_most_frequent': 'universal music group, inc.'}
```

The AssigneeNameMentions are created from the AssigneeMentions
using the [build_assignee_name_mentions_sql.py](../pv/disambiguation/assignee/build_assignee_name_mentions_sql.py)
script. This script simply groups AssigneeMentions together based
on their spelling.


### Featurizing Data

In order to cluster the data, we will need to build a set of features
that will be used to compute the similarity between two records:

To do so,  we need to load the model which will provide
the features:

```Python
from pv.disambiguation.assignee.model import AssigneeModel
encoding_model = AssigneeModel.from_config(config)
```

We can inspect what the features are of the model:

```Python
print(list(map(encoding_model.feature_list),lambda x: x.name)))
```

Notice that the model uses these features:

```
['entity_kb_feat', 'name_tfidf']
```

Each feature has a name and information about how it is computed:

```Python
# (encoder, computation, how to aggregate feature in cluster, is must link constraint, is must not link constraint)
(entity_kb_feat, FeatCalc.NO_MATCH, CentroidType.BINARY, False, True),
(name_tfidf, FeatCalc.DOT, CentroidType.NORMED, False, False)]
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
[('entitykb',
  True,
  1,
  array([[ 882261],
         [3229796],
         [     -1],
         [     -1],
         [     -1],
         [     -1],
         [     -1],
         [     -1],
         [3799812],
         [     -1]], dtype=int32),
  <FeatCalc.NO_MATCH: 4>,
  <CentroidType.BINARY: 1>),
 ('name_tfidf',
  False,
  22308973,
  <10x22308973 sparse matrix of type '<class 'numpy.float32'>'
        with 1163 stored elements in Compressed Sparse Row format>,
  <FeatCalc.DOT: 2>,
  <CentroidType.NORMED: 2>)]
```

These features will form the basis of our clustering model.

### Clustering model

To do the clustering, we will need to first define our
pairwise similarity model, which takes as input
sets of features:

```Python
# we don't have a trained model, we have a hand tuned one on the above features
model = AssigneeModel.from_config(config)
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

Each canopy is defined by a 4-gram that starts a word
 in the assignee name e.g., `'musi'` in `Universal Music`.

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
Canopies for inventors are given by strings such as `'musi'`.

Notice that for assignees the canopies are overlapping.
Notice that the mention above has four canopies:

```
 'canopies': {'musi', 'rsal', 'univ', 'usic'},
```

A clustering decision will be made for the mention
independently for each of the canopies, e.g.

```
MID                                      Canopy     Cluster_id
25fa40c1-16f3-4a6c-96e1-51dcf8a58dc6     musi       musi-10
25fa40c1-16f3-4a6c-96e1-51dcf8a58dc6     rsal       rsal-1
25fa40c1-16f3-4a6c-96e1-51dcf8a58dc6     univ       univ-3
25fa40c1-16f3-4a6c-96e1-51dcf8a58dc6     usic       usic-5
```

After these clustering decisions are made, we will merge
together all of the clusters with an overlapping mention, e.g.
`musi-10`, `rsal-1`, `univ-3`, `usic-5` will be merged.


### Storage of Results

We will store the flat clustering results. These will
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

There will be one file for each chunk.

## Putting it all Together

### Canopies & Featurizers

```bash
python -m pv.disambiguation.assignee.build_assignee_name_mentions_sql
```

### Running Full Batch Clustering

```
python -m pv.disambiguation.assignee.run_clustering
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
