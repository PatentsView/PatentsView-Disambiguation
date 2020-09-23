
import pickle

from absl import logging
from grinch.features import EncodingModel
from grinch.features import FeatCalc, CentroidType
from grinch.features import SingleItemHashingVectorizerFeatures, FastTextFeatures, HashingVectorizerFeatures


class InventorModel(object):

    @staticmethod
    def from_flags(flgs):
        logging.info('Building Inventor Model...')

        with open(flgs.patent_titles, 'rb') as fin:
            patent_tile_map = pickle.load(fin)
        logging.info('Loaded Patent Title Map...')
        for idx,(k,v) in enumerate(patent_tile_map.items()):
            logging.info('%s: %s', k, v)
            if idx > 5:
                break

        def get_patent_title(x):
            if x.record_id in patent_tile_map:
                logging.log_first_n(logging.INFO, 'Returning title for %s: %s', 10, x.record_id, patent_tile_map[x.record_id])
                x.title = patent_tile_map[x.record_id]
                return patent_tile_map[x.record_id]
            else:
                x.title = ''
                logging.warning('Missing title for %s', x.record_id)
                return ''

        with open(flgs.coinventors, 'rb') as fin:
            coinventor_map = pickle.load(fin)
        logging.info('Loaded Patent Coinventors Map...')
        for idx,(k,v) in enumerate(coinventor_map.items()):
            logging.info('%s: %s', k, str(v))
            if idx > 5:
                break

        def get_patent_coinventors(x):
            if x.record_id in coinventor_map:
                logging.log_first_n(logging.INFO, 'Returning coinventors for %s: %s', 10, x.record_id, coinventor_map[x.record_id])
                x.coinventors = coinventor_map[x.record_id]
                return coinventor_map[x.record_id]
            else:
                x.coinventors = []
                logging.warning('Missing coinventors for %s', x.patent_id)
                return []

        with open(flgs.assignees, 'rb') as fin:
            assignees_map = pickle.load(fin)
        logging.info('Loaded Patent Assignees Map...')
        for idx,(k,v) in enumerate(assignees_map.items()):
            logging.info('%s: %s', k, str(v))
            if idx > 5:
                break

        def get_patent_assignees(x):
            if x.record_id in assignees_map:
                logging.log_first_n(logging.INFO, 'Returning assignees for %s: %s', 10, x.record_id, assignees_map[x.record_id])
                x.assignees = assignees_map[x.record_id]
                return assignees_map[x.record_id]
            else:
                x.assignees = []
                logging.log_first_n(logging.WARNING, 'Missing assignees for %s', 10, x.record_id)
                return []

        # Name features
        first_initial = SingleItemHashingVectorizerFeatures('first_initial', lambda x: x.first_initial())
        first_name = SingleItemHashingVectorizerFeatures('first_name', lambda x: x.first_name())
        suffix = SingleItemHashingVectorizerFeatures('suffix', lambda x: x.suffixes())
        middle_initial = SingleItemHashingVectorizerFeatures('middle_initial', lambda x: x.middle_initial())
        middle_name = SingleItemHashingVectorizerFeatures('middle_name', lambda x: x.middle_name())

        canopy_feat = SingleItemHashingVectorizerFeatures('canopy', lambda x: x.canopy())

        # Title Features
        title_model = FastTextFeatures(flgs.title_model, 'title', get_patent_title)

        # Co-Inventor Features
        coinventors = HashingVectorizerFeatures('coinventors', lambda x: get_patent_coinventors(x), 'l2')

        assignees = HashingVectorizerFeatures('assignees', lambda x: get_patent_assignees(x), 'l2')

        # Lawyer Features

        # PatentID Features
        patent_id = HashingVectorizerFeatures('patentid', lambda x: x.record_id)

        triples = [(title_model, FeatCalc.DOT, CentroidType.NORMED, False, False),
                   (first_name, FeatCalc.NO_MATCH, CentroidType.BINARY, False, True),
                   (middle_initial, FeatCalc.NO_MATCH, CentroidType.BINARY, False, True),
                   (middle_name, FeatCalc.NO_MATCH, CentroidType.BINARY, False, True),
                   (suffix, FeatCalc.NO_MATCH, CentroidType.BINARY, False, True),
                   (canopy_feat, FeatCalc.NO_MATCH, CentroidType.BINARY, False, True),
                   (coinventors, FeatCalc.DOT, CentroidType.NORMED, False, False),
                   (assignees, FeatCalc.DOT, CentroidType.NORMED, False, False)]
        encoders = [t[0] for t in triples]
        feature_types = [t[1] for t in triples]
        centroid_types = [t[2] for t in triples]
        must_links = set([t[0].name for t in triples if t[3]])
        must_not_links = set([t[0].name for t in triples if t[4]])
        assert len(encoders) == len(feature_types)
        assert len(feature_types) == len(centroid_types)
        return EncodingModel(encoders,
                             'InventorModelWithApps',
                             {},
                             feature_types, centroid_types, must_links, must_not_links)

