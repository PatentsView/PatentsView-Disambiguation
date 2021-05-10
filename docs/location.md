# Location Disambiguation

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

Similar to the assignee case, we will be using two different
data structures to store location mentions. The first is
[LocationMention](../pv/disambiguation/core.py#L174) which corresponds
to a row in the rawlocation table. To make our methods more efficient
we make an assumption that for a given assignee or inventor,
all locations with the same (exact) spelling refer to the same
location.

In summary,
- [LocationMention](../pv/disambiguation/core.py#L174) - Corresponds to one row in the rawlocation table
- [LocationNameMention](../pv/disambiguation/core.py#L312) - Corresponds to one unique string spelling + inventor/assignee ID pair of a location

We load the `LocationNameMention`s from a precomputed pickle file of data.

```Python
from pv.disambiguation.core import LocationNameMention
```

```Python
class Loader(object):
    def __init__(self, name_mentions):
        self.name_mentions = name_mentions

    def load(self, canopy):
        return self.name_mentions[canopy]

    def num_records(self, canopy):
        return len(self.name_mentions[canopy])

    @staticmethod
    def from_config(config):
        with open(config['location']['inventor_location_name_mentions'], 'rb') as fin:
            inventor_name_mentions = pickle.load(fin)
        with open(config['location']['assignee_location_name_mentions'], 'rb') as fin:
            assignee_name_mentions = pickle.load(fin)
        name_mentions = inventor_name_mentions
        name_mentions.update(assignee_name_mentions)
        l = Loader(name_mentions)
        return l
```

For instance,

```Python
import configparser
from pv.disambiguation.location.load import Loader

config = configparser.ConfigParser()
config.read(['config/database_config.ini', 'config/location/run_clustering.ini',
             'config/database_tables.ini'])

loader = Loader.from_config(config)

mentions = loader.load(canopy)
```

How do we create this pickle file of `LocationNameMentions`?

**Canopies** We first need to split locations into canopies. The canopies
for locations are defined by the *disambiguated* ID of the inventors
and assignees associated with the location.

This data is computed by first computing the canopies

```
python -m pv.disambiguation.location.build_assignee_location_canopies
python -m pv.disambiguation.location.build_inventor_location_canopies
```

For each canopy we group `LocationMentions` into `LocationNameMentions`
using these scripts:

```
python -m pv.disambiguation.location.build_assignee_location_mentions
python -m pv.disambiguation.location.build_inventor_location_mentions
```

## Featurizing Data

Now let's look at the features used for the location
mentions. Using the same structures as the inventor/assignee
structures:

```
(city, FeatCalc.LOCATION, CentroidType.NORMED, False, False),
(city_sim, FeatCalc.DOT, CentroidType.NORMED, False, False),
(state, FeatCalc.LOCATION, CentroidType.BINARY, False, False),
(country, FeatCalc.LOCATION, CentroidType.BINARY, False, False),
(db, FeatCalc.LOCATION, CentroidType.BINARY, False, False),
(num_mentions, FeatCalc.LOCATION, CentroidType.BINARY, False, False),
```

These features correspond to:

* The city name itself
* A city name similarity feature
* The state name itself
* The country name itself
* A feature representing whether the location matched a record in the database
* Number of `LocationMentions` combined to form this `LocationNameMention`


## Clustering Data

We use a custom similarity for LocationData.
This is a rule-based method:

```
class LocationAgglom(Agglom):
    def __init__(self, model, features, num_points):
        super(LocationAgglom, self).__init__(model, features, num_points, min_allowable_sim=0.0)
        self.relaxed_sim_threshold = 0.4

    def csim_multi_feature_knn_torch(self, i, j, record_dict=None):
        len_i = len(i)
        len_j = len(j)
        s = torch.zeros((len_i, len_j), dtype=torch.float32)

        # rule 1
        # no mismatch in terms of the spellings and database
        # city_match = np.logical_and(np.logical_and(self.dense_features[self.dense_feature_id['city']][3][i] ==
        #                                            self.dense_features[self.dense_feature_id['city']][3][j].T,
        #                                            self.dense_features[self.dense_feature_id['city']][3][i] != -1),
        #                             self.dense_features[self.dense_feature_id['city']][3][j].T != -1)

        city_relaxed_match = (self.sparse_features[self.sparse_feature_id['city_sim']][3][i]
                              @ self.sparse_features[self.sparse_feature_id['city_sim']][3][j].T).todense().A > self.relaxed_sim_threshold


        country_match = np.logical_or(np.logical_or(self.dense_features[self.dense_feature_id['country']][3][i] ==
                                                    self.dense_features[self.dense_feature_id['country']][3][j].T,
                                                    self.dense_features[self.dense_feature_id['country']][3][i] == -1),
                                      self.dense_features[self.dense_feature_id['country']][3][j].T != -1)

        state_match = np.logical_or(np.logical_or(self.dense_features[self.dense_feature_id['state']][3][i] ==
                                                  self.dense_features[self.dense_feature_id['state']][3][j].T,
                                                  self.dense_features[self.dense_feature_id['state']][3][i] == -1),
                                    self.dense_features[self.dense_feature_id['state']][3][j].T != -1)

        # each feature is 1 if there is a problem, 0 otherwise.
        rule_1 = np.logical_and(city_relaxed_match, np.logical_and(country_match, state_match))

        # rule 2
        # city match & one in database, city is name
        one_in_db = np.logical_xor(self.dense_features[self.dense_feature_id['db']][3][i] != -1,
                                   self.dense_features[self.dense_feature_id['db']][3][j].T != -1)

        rule_2 = np.logical_and(city_relaxed_match, one_in_db)

        # rule 3
        # num mentions of one is 1.5 times more than the other
        # city name is the same.

        num_mentions_ratio = self.c_ratio_dense_knn(None,
                                                    self.dense_features[self.dense_feature_id['num_mentions']][3][i],
                                                    self.dense_features[self.dense_feature_id['num_mentions']][3][j])

        rule_3 = np.logical_and(city_relaxed_match, np.logical_or(num_mentions_ratio > 1.5, num_mentions_ratio < 1.5))

        s += torch.from_numpy(np.logical_or(rule_1, np.logical_or(rule_2, rule_3)).astype(np.float32))

        return s
```

## Storage of Results

This follows the same methodology as the assignee case.