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

### Storage of data

### Training a model

## Pain Points

### Memory Usage

### Disk Usage

### Chunking of canopies

## Proposed Changes

### Reduce Feature Map sizes

### Run Specific Canopies

### Sparse + Constraints
