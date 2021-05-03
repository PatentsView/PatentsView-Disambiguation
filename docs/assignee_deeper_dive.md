# Assignee Disambiguation

## Features & Model

Each record is represented by a set of features.
These features are defined in the [AssigneeModel](../pv/disambiguation/assignee/model.py)

There are 2 kinds of features that we use:
(1) "Must-not link" features,
(2) dense embedding cosine similarity features,

**Must-not link** For example, we have a feature which indicates if two
inventor names have _different_ PermID entities.
We attempt to link assignee names into PermID using the following [logic](../pv/disambiguation/assignee/model.py#L13)

```Python
entity_kb_feat = EntityKBFeatures('resources/permid_entity_info.pkl', 'entitykb', lambda x: x)
```

**Sparse Cosine Similarity** We use a TF-IDF based sparse representation of the n-grams in an assignee string.
 We again have a wrapper class for computing this feature:

```Python
name_tfidf = SKLearnVectorizerFeatures(config['assignee']['assignee_name_model'],
                                       'name_tfidf',
                                       lambda x: clean(split(x.normalized_most_frequent)))
```

which is defined as:

```Python
class SKLearnVectorizerFeatures(object):
    """Features for SKLearn vectorizer."""

    def __init__(self, filename, name, get_field):
        self.filename = filename
        self.name = name
        self.get_field = get_field
        logging.info('Loading model from %s...' % filename)
        t = time.time()
        with open(self.filename, 'rb') as fin:
            self.model = pickle.load(fin)
        logging.info('Finished loading %s.', time.time() - t)

    def encode(self, things_to_encode):
        logging.log_first_n(logging.INFO, 'len(things_to_encode) = %s, %s', 10, len(things_to_encode),
                            ', '.join([str(self.get_field(x)) for x in things_to_encode[:5]]))
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


