# Inventor Disambiguation

## Topics

### Features & Model

Each record is represented by a set of features.
These features are defined in the [InventorModel](../pv/disambiguation/inventor/model.py)

There are 3 main kinds of features that we use:
(1) "Must-not link" features,
(2) dense embedding cosine similarity features,
(3) sparse bag-of-words similarity features.

**Must-not link** For example, we have a feature which indicates if two
inventor names have _different_ full middle names. For instance,
we say that `John Adams Smith` and `John Arthur Smith` have
different middle names, but `John A. Smith` does not have a different
middle name as compared to `John Adams Smith` and `John Arthur Smith`.
To implement this we represent each middle name with an integer
provided by a hash function and use `-1` for missing values (e.g.,
no middle name). We wrap this in a class:

```Python
middle_name = SingleItemHashingVectorizerFeatures('middle_name', lambda x: x.middle_name())
```

which is defined as:

```Python
class SingleItemHashingVectorizerFeatures(object):
    """Features for hashing vectorizer where each feature is a single integer."""

    def __init__(self, name, get_field, norm=None):
        self.name = name
        self.get_field = get_field
        from sklearn.feature_extraction.text import HashingVectorizer
        self.model = HashingVectorizer(analyzer=lambda x: [' '.join(x)] if x else [], alternate_sign=False,
                                       dtype=np.int32, norm=None)

    def encode(self, things_to_encode):
        res = self.model.transform([self.get_field(x) for x in things_to_encode])
        idx, val = res.nonzero()
        enc = -1 * np.ones(len(things_to_encode), dtype=np.int32)
        enc[idx] = val
        return np.expand_dims(enc, axis=1)
```

The reason we do this to utilize numpy's efficient matrix multiplications when computing all pairwise comparisons:

```Python
def all_pairs_no_match_feature(self, c_i, c_j):
    """

    Args:
        c_i - N by 1 vector of features
        c_j - M by 1 vector of features
    Returns:
        N by M matrix, where position i, j is 1 if the two features do not match
    """
    c_jT = c_j.T
    res = np.logical_and(np.logical_and(c_i != c_jT, c_i != -1), c_jT != -1)
    return torch.from_numpy(res.astype(np.float32))
```

**Sparse Cosine Similarity** For example, we model the overlapping co-inventors
of two records. We again have a wrapper class for computing this feature:

```Python
coinventors = HashingVectorizerFeatures('coinventors', lambda x: get_patent_coinventors(x), 'l2')
```

which is defined as:

```Python
class HashingVectorizerFeatures(object):
    """Features for hashing vectorizer."""

    def __init__(self, name, get_field, norm=None):
        self.name = name
        self.get_field = get_field
        from sklearn.feature_extraction.text import HashingVectorizer
        self.model = HashingVectorizer(analyzer=lambda x: [xx for xx in x], alternate_sign=False, dtype=np.float32,
                                       norm=norm)

    def encode(self, things_to_encode):
        return self.model.transform([self.get_field(x) for x in things_to_encode])
```

The `encode` method here will return a [scipy CSR matrix](https://docs.scipy.org/doc/scipy/reference/generated/scipy.sparse.csr_matrix.html)
which we can then use to compute pairwise affinities efficiently
using matrix multiplication as well:

```Python
def c_dot_feature_sparse(self, c_i, c_j):
    """

    Args:
        c_i - N by d matrix of sparse features (CSR)
        c_j - M by d matrix of sparse features (CSR)
    Returns:
        N by M matrix, where position i, j is the similarity of C_i[i] and C_j[j]
    """
    res = (c_i @ c_j.T).todense().A
    return torch.from_numpy(res.astype(np.float32))
```

**Dense Cosine Similarity** We represent patent titles
using an embedding produced by a [Sent2Vec](https://github.com/epfml/sent2vec)
model trained (build on top of FastText) on all of the patent data. We do this
by:

```Python
title_model = FastTextFeatures(config['inventor']['title_model'], 'title', get_patent_title)
```

Computing pairwise similarities efficiently can be done using
matrix operations as well.

Overall, we use these features:

```Python
# (name, computation, how to aggregate feature in cluster, is must link constraint, is must not link constraint)
[(title_model, FeatCalc.DOT, CentroidType.NORMED, False, False),
    (first_name, FeatCalc.NO_MATCH, CentroidType.BINARY, False, True),
    (middle_initial, FeatCalc.NO_MATCH, CentroidType.BINARY, False, True),
    (middle_name, FeatCalc.NO_MATCH, CentroidType.BINARY, False, True),
    (suffix, FeatCalc.NO_MATCH, CentroidType.BINARY, False, True),
    (canopy_feat, FeatCalc.NO_MATCH, CentroidType.BINARY, False, True),
    (coinventors, FeatCalc.DOT, CentroidType.NORMED, False, False),
    (assignees, FeatCalc.DOT, CentroidType.NORMED, False, False)]
```

Given these features on pairs of records, we can efficiently score
pairs of records using a linear model. We implement this using pytorch.
This linear model is defined [here](https://github.com/iesl/grinch/blob/main/src/python/grinch/model.py#L23-L70).
Given two sets of records `C_i` (N records) and `C_i` (M records),
we can compute the pairwise similarity between all pairs by
computing the contribution for reach feature independently and then
using the linear model to combine them as seen [here](https://github.com/iesl/grinch/blob/main/src/python/grinch/agglom.py#L115-L139).

### Clustering Algorithms

We use [hierarchical agglomerative clustering (HAC)](https://docs.scipy.org/doc/scipy/reference/generated/scipy.cluster.hierarchy.linkage.html)
for clustering in the batch setting and [Grinch](https://github.com/iesl/grinch) for the incremental setting.

Let's start by discussing HAC. To cluster a canopy of `N` points,
we will need to build the full `N^2` similarity matrix, which is
done using the model described above. We use a wrapper around
scipy's [linkage](https://docs.scipy.org/doc/scipy/reference/generated/scipy.cluster.hierarchy.linkage.html) function [agglom.py](https://github.com/iesl/grinch/blob/main/src/python/grinch/agglom.py).

This wrapper class looks like this:

```Python
class Agglom(object):
    """ Hierarchical agglomerative clustering with learned linear model w/ rules."""

    def __init__(self, model, features, num_points, min_allowable_sim=-20000.0):
        """ Constructor for Agglom.

        :param model: Pairwise linear scoring function
        :param features: features used in linear model.
        :param num_points: number of points that we are clustering
        :param min_allowable_sim: minimum allowable similarity (used for rule-based models)
        """

        self.model = model
        self.features = features
        self.num_points = num_points

        logging.info('Using len(features)=%s', len(features))
        self.dense_features = [(fn, is_dense, dim, feat_mat, feat_calc, centroid_type)
                               for fn, is_dense, dim, feat_mat, feat_calc, centroid_type in features if is_dense]
        self.sparse_features = [(fn, is_dense, dim, feat_mat, feat_calc, centroid_type)
                                for fn, is_dense, dim, feat_mat, feat_calc, centroid_type in features if not is_dense]

        logging.info('%s dense features: %s', len(self.dense_features), ', '.join([x[0] for x in self.dense_features]))
        logging.info('%s sparse features: %s', len(self.sparse_features),
                     ', '.join([x[0] for x in self.sparse_features]))

        self.min_allowable_sim = min_allowable_sim

        self.dense_point_features = []
        self.sparse_point_features = []
        self.dense_feature_id = dict()
        for idx, (fn, is_dense, dim, feat_mat, _, _) in enumerate(self.dense_features):
            self.dense_feature_id[fn] = idx
            self.dense_point_features.append(feat_mat)

        self.sparse_feature_id = dict()
        for idx, (fn, is_dense, dim, feat_mat, _, _) in enumerate(self.sparse_features):
            self.sparse_feature_id[fn] = idx
            # analyze_sparsity(feat_mat, fn)
            self.sparse_point_features.append(feat_mat)

        self.dists = None
        self.Z = None

    def all_thresholds(self):
        """All possible ways to cut a tree."""
        return self.Z[:, 2]

    def flat_clustering(self, threshold):
        """A flat clustering extracted from the tree by cutting at the given distance threshold.

        :param threshold: distance threshold
        :return: res - N element array such that res[i] is the cluster id of the ith point.
        """
        return fcluster(self.Z, threshold, criterion='distance')

    def build_dendrogram_hac(self):
        """Run HAC inference."""
        st_sims = time.time()
        sims = self.csim_multi_feature_knn_batched(np.arange(self.num_points), np.arange(self.num_points))
        self.sims = sims
        en_sims = time.time()
        logging.info('Time to compute sims: %s', en_sims - st_sims)
        logging.info('Finished batched similarities!')
        st_prep = time.time()
        pos_sim = np.maximum(sims, self.min_allowable_sim) - np.minimum(0.0, self.min_allowable_sim)
        dists = 1 / (1 + pos_sim)
        dists = (dists + dists.T) / 2.0
        np.fill_diagonal(dists, 0.0)
        dists = squareform(dists)
        self.dists = dists
        en_prep = time.time()
        logging.info('Time to compute sims: %s', en_prep - st_prep)
        logging.info('Finished preparing distances!')
        logging.info('Running hac')
        st_linkage = time.time()
        Z = linkage(dists, method='average')
        self.Z = Z
        en_linkage = time.time()
        logging.info('Time to run HAC: %s', en_linkage - st_linkage)
        logging.info('Finished hac!')
```

HAC will produce a tree structure. We then will extract a flat clustering
using `flat_clustering` which cuts the tree according to a given threshold.

The HAC algorithm does not define how to add incrementally arriving
data to an existing tree structure and so we use Grinch which provides
incremental updates.

We will serialize the tree structure built using HAC:

```Python
logging.info('Beginning to save all tree structures....')
grinch_trees = []
for idx,t in tqdm(enumerate(tree_list), total=len(tree_list)):
    grinch = WeightedMultiFeatureGrinch.from_agglom(t, pids_list[idx])
    grinch.prepare_for_save()
    grinch_trees.append(grinch)
torch.save([grinch_trees, canopy2tree_id], outstatefile)
```

This serialized tree stores all of the features and all of the information
about the mentions needed to provide an updated disambiguation. Notable
it has the UUIDs (pids).

`TODO: Walk through of update procedure`

### Storage of data

### Training a model

We have a script to train the pairwise model:

```
python -m pv.disambiguation.inventor.train_model --training_data data/inventor-train/eval_common_characteristics.train --dev_data data/inventor-train/eval_common_characteristics.dev --max_num_dev_canopies 200
```

`TODO: Additional details`

## Pain Points & Remedies

### Memory Usage

High memory usage comes from:

1. Feature maps sitting in memory
2. Sent2Vec model sitting in memory
3. Computing full O(N^2) similarity matrix for HAC.

To remedy this, we will:
1. On-the-fly construct the feature maps using SQL queries
2. Reduce dimensionality of Sent2Vec model or [quantize](https://fasttext.cc/docs/en/faqs.html) it
3. Switch to HAC / Grinch using a sparse k-NN graph ~O(NK) space (typically, comparable or better accuracy in practice)

### Disk Usage

High disk usage comes from:
1. Storing the featurized representations in the serialized trees for incremental updates

To remedy this we will:
1. We will not store featurized representations, but recompute features using the on-the-fly feature maps described above.

### Static, file-based chunking of canopies

Debugging and organization issue:

1. Currently we create a partition of the data by canopy that is static
and causes bulky chunk-based parallelism that can require too much recomputation when debugging

To remedy this we will:
1. Store serialized tree structures in a database rather than pickle files
