import numpy as np
import torch
from absl import logging
from grinch.agglom import Agglom
from grinch.features import EncodingModel
from grinch.features import FeatCalc, CentroidType
from grinch.features import SingleItemHashingVectorizerFeatures

from pv.disambiguation.location.reparser import LOCATIONS


class LocationDatabaseFeatures(object):
    def __init__(self, name, get_field, norm=None):
        self.name = name
        self.get_field = get_field
        LOCATIONS.load()

    def encode(self, things_to_encode):
        res = -1 * np.ones(len(things_to_encode), dtype=np.int32)
        for idx, x in enumerate(things_to_encode):
            if x in LOCATIONS:
                x._in_database = 1
                res[idx] = 1
            else:
                x._in_database = 0
        return np.expand_dims(res, axis=1)


class NumMentionsLocationFeatures(object):
    def __init__(self, name, get_field, norm=None):
        self.name = name
        self.get_field = get_field

    def encode(self, things_to_encode):
        res = np.ones(len(things_to_encode), dtype=np.int32)
        for idx, x in enumerate(things_to_encode):
            res[idx] = x.num_records

        return np.expand_dims(res, axis=1)


class LocationModelWithApps(object):
    @staticmethod
    def from_flags(flgs):
        logging.info('Building Location Model...')

        city = SingleItemHashingVectorizerFeatures('city', lambda x: x._canonical_city)
        state = SingleItemHashingVectorizerFeatures('state', lambda x: x._canonical_state)
        country = SingleItemHashingVectorizerFeatures('country', lambda x: x._canonical_country)

        # location Features
        db = LocationDatabaseFeatures('db', lambda x: x)

        # location Features
        num_mentions = NumMentionsLocationFeatures('num_mentions', lambda x: x)

        triples = [(city, FeatCalc.LOCATION, CentroidType.NORMED, False, False),
                   (state, FeatCalc.LOCATION, CentroidType.BINARY, False, False),
                   (country, FeatCalc.LOCATION, CentroidType.BINARY, False, False),
                   (db, FeatCalc.LOCATION, CentroidType.BINARY, False, False),
                   (num_mentions, FeatCalc.LOCATION, CentroidType.BINARY, False, False),
                   ]
        encoders = [t[0] for t in triples]
        feature_types = [t[1] for t in triples]
        centroid_types = [t[2] for t in triples]
        must_links = set([t[0].name for t in triples if t[3]])
        must_not_links = set([t[0].name for t in triples if t[4]])
        assert len(encoders) == len(feature_types)
        assert len(feature_types) == len(centroid_types)
        return EncodingModel(encoders,
                             'LocationModelWithApps',
                             {},
                             feature_types, centroid_types, must_links, must_not_links)


class LocationAgglom(Agglom):
    def __init__(self, model, features, num_points):
        super(LocationAgglom, self).__init__(model, features, num_points)

    def csim_multi_feature_knn_torch(self, i, j, record_dict=None):
        len_i = len(i)
        len_j = len(j)
        s = torch.zeros((len_i, len_j), dtype=torch.float32)

        # rule 1
        # no mismatch in terms of the spellings and database
        city_match = np.logical_and(np.logical_and(self.dense_features[self.dense_feature_id['city']][3][i] ==
                                                   self.dense_features[self.dense_feature_id['city']][3][j].T,
                                                   self.dense_features[self.dense_feature_id['city']][3][i] != -1),
                                    self.dense_features[self.dense_feature_id['city']][3][j].T != -1)

        country_match = np.logical_or(np.logical_or(self.dense_features[self.dense_feature_id['country']][3][i] ==
                                                    self.dense_features[self.dense_feature_id['country']][3][j].T,
                                                    self.dense_features[self.dense_feature_id['country']][3][i] == -1),
                                      self.dense_features[self.dense_feature_id['country']][3][j].T != -1)

        state_match = np.logical_or(np.logical_or(self.dense_features[self.dense_feature_id['state']][3][i] ==
                                                  self.dense_features[self.dense_feature_id['state']][3][j].T,
                                                  self.dense_features[self.dense_feature_id['state']][3][i] == -1),
                                    self.dense_features[self.dense_feature_id['state']][3][j].T != -1)

        # each feature is 1 if there is a problem, 0 otherwise.
        rule_1 = np.logical_and(city_match, np.logical_and(country_match, state_match))

        # rule 2
        # city match & one in database, city is name
        one_in_db = np.logical_xor(self.dense_features[self.dense_feature_id['db']][3][i] != -1,
                                   self.dense_features[self.dense_feature_id['db']][3][j].T != -1)

        rule_2 = np.logical_and(city_match, one_in_db)

        # rule 3
        # num mentions of one is 1.5 times more than the other
        # city name is the same.

        num_mentions_ratio = self.c_ratio_dense_knn(None,
                                                    self.dense_features[self.dense_feature_id['num_mentions']][3][i],
                                                    self.dense_features[self.dense_feature_id['num_mentions']][3][j])

        rule_3 = np.logical_and(city_match, np.logical_or(num_mentions_ratio > 1.5, num_mentions_ratio < 1.5))

        # rule 4 - TODO
        # num mentions of one is 1.5 times more than the other
        # city name intersection of tokens.

        # rule 5 - TODO
        # Inventor / assignee

        s += torch.from_numpy(np.logical_or(rule_1, np.logical_or(rule_2, rule_3)).astype(np.float32))

        return s
