import collections


from absl import flags
from absl import logging
from absl import app

from tqdm import tqdm

FLAGS = flags.FLAGS

flags.DEFINE_string('input', '/iesl/data/patentsview/2019-12-31/patent.tsv', '')


def same_inventor_same_patent(inventor_clusters):
    for c in tqdm(inventor_clusters.values(), desc='inventor_clusters'):
        patent_ids = sorted([x.split('-')[0] for x in c])
        logging.log_first_n(logging.INFO, 'patent_ids = %s', 10, str(patent_ids))
        logging.log_first_n(logging.INFO, 'len(patent_ids) = %s, len(set(patent_ids)) %s', 10, len(patent_ids), len(set(patent_ids)))
        assert len(patent_ids) == len(set(patent_ids)), 'Two names on same patent for the same inventor %s, (%s, %s)' % (str(patent_ids),  len(patent_ids), len(set(patent_ids)))
    logging.info('Check passed!')


def load_inventor_clusters(filename, mention_col=0, entity_col=1):
    res = collections.defaultdict(list)
    with open(filename, 'r') as fin:
        for idx,line in enumerate(fin):
            splt = line.split('\t')
            if len(splt) >= 2:
                res[splt[entity_col]].append(splt[mention_col])
    return res

def main(argv):
    logging.info('Running with args %s', str(argv))
    c = load_inventor_clusters(FLAGS.input)
    same_inventor_same_patent(c)

if __name__ == "__main__":
    app.run(main)
