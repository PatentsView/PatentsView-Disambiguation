import os
import pickle
import time

import numpy as np
from absl import logging
from grinch.features import EncodingModel
from grinch.features import FeatCalc, CentroidType
from grinch.features import HashingVectorizerFeatures, SKLearnVectorizerFeatures

from pv.disambiguation.assignee.names import normalize_name, clean, split, remove_stopwords
from sklearn.feature_extraction.text import TfidfVectorizer


class LocationVectorizer(object):
    """Features for hashing vectorizer."""

    def __init__(self, name, get_field, norm=None):
        self.name = name
        self.get_field = get_field

    def encode(self, things_to_encode):
        import scipy

        return scipy.sparse.csr_matrix([self.get_field(x) for x in things_to_encode])


class EntityKBFeatures(object):
    def __init__(self, entity_info_file, name, get_field, norm=None):
        logging.info('building entity kb...')
        with open(entity_info_file, 'rb') as f:
            [self.entity_ids, self.entity_names] = pickle.load(f)
        self.emap = dict()
        self.missing_entities = ['army', 'navy']
        if not os.path.exists(entity_info_file + '.cache.pkl'):
            for idx in range(len(self.entity_ids)):
                logging.log_first_n(logging.INFO, 'entity kb: %s -> %s', 10, self.entity_names[idx], idx)
                logging.log_every_n_seconds(logging.INFO, 'entity kb: %s of %s', 10, idx, len(self.entity_ids))
                self.emap[self.entity_names[idx].lower()] = idx
                normalized = normalize_name(self.entity_names[idx])
                splt = split(normalized)
                cleaned = clean(splt)
                nostop = remove_stopwords(cleaned)
                if normalized not in self.emap:
                    self.emap[normalized] = idx
                if splt not in self.emap:
                    self.emap[splt] = idx
                if cleaned not in self.emap:
                    self.emap[cleaned] = idx
                if nostop not in self.emap:
                    self.emap[nostop] = idx
            for me in self.missing_entities:
                self.emap[me] = len(self.emap)
            with open(entity_info_file + '.cache.pkl', 'wb') as fout:
                pickle.dump(self.emap, fout)
        else:
            with open(entity_info_file + '.cache.pkl', 'rb') as fin:
                self.emap = pickle.load(fin)
        self.name = name
        self.get_field = get_field
        logging.info('building entity kb...done')

    def encode(self, things_to_encode):
        res = -1 * np.ones(len(things_to_encode), dtype=np.int32)
        for idx, x in enumerate(things_to_encode):
            if x.normalized_most_frequent in self.emap:
                logging.log_first_n(logging.INFO, 'in entity kb (normalized): %s %s', 10, x.normalized_most_frequent,
                                    self.emap[x.normalized_most_frequent])
                res[idx] = self.emap[x.normalized_most_frequent]
            else:
                splt_x = split(x.normalized_most_frequent)
                cleaned = clean(splt_x)
                # nostop = remove_stopwords(cleaned)
                if splt_x in self.emap:
                    logging.log_first_n(logging.INFO, 'in entity kb (split): %s %s', 10, splt_x, self.emap[splt_x])
                    res[idx] = self.emap[splt_x]
                elif cleaned in self.emap:
                    logging.log_first_n(logging.INFO, 'in entity kb (cleaned: %s %s', 10, cleaned, self.emap[cleaned])
                    res[idx] = self.emap[cleaned]
                # elif nostop in self.emap:
                #     logging.log_first_n(logging.INFO, 'in entity kb (nostop): %s %s', 10, nostop, self.emap[nostop])
                #     res[idx] = self.emap[nostop]
        return np.expand_dims(res, axis=-1)


class CustomSKLearnVectorizerFeatures(object):
    """Features for SKLearn vectorizer."""

    def __init__(self, filename, name, get_field):
        self.filename = filename
        self.name = name
        self.get_field = get_field
        logging.info('Loading model from %s...' % filename)
        t = time.time()
        import dill as dpickle

        # from pv.disambiguation.assignee.assignee_analyzer import REMAPPING_CONFIGURATION, CORRECTION_CONFIGURATION, \
        #     STOPPHRASE_CONFIGURATION, THRESHOLD, analyze_assignee_name
        with open(self.filename, 'rb') as fin:
            self.model = dpickle.load(fin)
        logging.info('Finished loading %s.', time.time() - t)

    def encode(self, things_to_encode):
        import editdistance
        from pv.disambiguation.assignee.assignee_analyzer import analyze_assignee_name

        logging.log_first_n(logging.INFO, 'len(things_to_encode) = %s, %s', 10, len(things_to_encode),', '.join([str(self.get_field(x)) for x in things_to_encode[:5]]))
        return self.model.transform([analyze_assignee_name(self.get_field(x)) for x in things_to_encode])

class LocationVectorizerFeatures(object):
    """Features for hashing vectorizer."""
    def __init__(self, name, get_field, norm=None):
        self.name = name
        self.get_field = get_field
        from sklearn.feature_extraction.text import CountVectorizer
        # self.model = CountVectorizer(tokenizer=lambda x: [' '.join(x)] if x else [])
        self.model = CountVectorizer(tokenizer=lambda x: x, lowercase=False)

    def encode(self, things_to_encode):
        a = [self.get_field(x) for x in things_to_encode]
        newlist = [list(x) for x in a]
        a = self.model.fit_transform(newlist).toarray()
        return self.model.fit_transform(newlist)


# class LocationVectorizerFeatures(object):
#     """Features for hashing vectorizer."""
#
#     def __init__(self, name, get_field, norm=None):
#         self.name = name
#         self.get_field = get_field
#         from sklearn.feature_extraction.text import HashingVectorizer
#         self.model = HashingVectorizer(analyzer=lambda x: [xx for xx in x], alternate_sign=False, dtype=np.float32, norm=norm, binary=True)
#
#     def encode(self, things_to_encode):
#         return self.model.transform([self.get_field(x) for x in things_to_encode])

class AssigneeModel(object):

    @staticmethod
    def from_config(config):
        logging.info('Building Assignee Model...')

        # Features:
        # name_features = HashingVectorizerFeatures('name_features', lambda x: x.name_features)
        locations = LocationVectorizerFeatures('locations', lambda x: x.location_strings, norm=None)

        canopy_feat = HashingVectorizerFeatures('canopy', lambda x: x.canopies)
        # entity_kb_feat = EntityKBFeatures('clustering_resources/permid_entity_info.pkl', 'entitykb', lambda x: x)
        # PatentID Feature
        # patent_id = HashingVectorizerFeatures('patentid', lambda x: x.record_id)
        name_tfidf = CustomSKLearnVectorizerFeatures(config['assignee']['assignee_name_model'],
                                                     'name_tfidf',
                                                     lambda x: clean(split(x.normalized_most_frequent)))
        latlong_vectorizer = LocationVectorizer('location_lat_long',
                                                lambda x: np.array([x.average_lat, x.average_lon]))
        triples = [
            (locations, FeatCalc.L2, CentroidType.NO_NORM, False, False),
            # (entity_kb_feat, FeatCalc.NO_MATCH, CentroidType.BINARY, False, True),
            (name_tfidf, FeatCalc.DOT, CentroidType.NORMED, False, False)]
        encoders = [t[0] for t in triples]
        feature_types = [t[1] for t in triples]
        centroid_types = [t[2] for t in triples]
        must_links = set([t[0].name for t in triples if t[3]])
        must_not_links = set([t[0].name for t in triples if t[4]])
        assert len(encoders) == len(feature_types)
        assert len(feature_types) == len(centroid_types)
        return EncodingModel(encoders, 'AssigneeModelWithApps', {}, feature_types, centroid_types, must_links, must_not_links)
