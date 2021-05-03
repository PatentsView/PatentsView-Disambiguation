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

There are two kinds of mentions that we will be using:

- [LocationMention](../pv/disambiguation/core.py#L174) - Corresponds to one row in the rawlocation table
- [LocationNameMention](../pv/disambiguation/core.py#L312) - Corresponds to one unique string spelling + inventor/assignee ID pair of a location

Recall that we assume that if two assignee names have identical spelling that the two are the same assignee.

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

```Python
import configparser
config = configparser.ConfigParser()
config.read(['config/database_config.ini', 'config/location/run_clustering.ini',
             'config/database_tables.ini'])

loader = Loader.from_config(config)

mentions = loader.load(canopy)
```

This data is computed by first computing the canopies

```
python -m pv.disambiguation.location.build_assignee_location_canopies
python -m pv.disambiguation.location.build_inventor_location_canopies
```

and then building the mentions

```
python -m pv.disambiguation.location.build_assignee_location_mentions
python -m pv.disambiguation.location.build_inventor_location_mentions
``
