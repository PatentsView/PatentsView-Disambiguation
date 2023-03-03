from sklearn.feature_extraction.text import TfidfVectorizer

import os
from absl import app
from absl import logging
import numpy as np
import pickle


def main(argv):
    logging.info('Running Build City Name Index with arguments: %s', str(argv))
    vectorizer = TfidfVectorizer(decode_error='replace', strip_accents='ascii', lowercase=True, ngram_range=(2,10),
                                 analyzer='char_wb', stop_words=None, binary=False,
                                 dtype=np.float32, max_df=1.0, min_df=1, max_features=None,
                                 use_idf=True, norm='l2', smooth_idf=True, sublinear_tf=False)

    name_file = argv[1]
    raw_names = []
    with open(name_file, 'r') as fin:
        for line in fin:
            raw_names.append(line.strip())

    logging.info('Finished raw data...')
    logging.info('\t'.join(raw_names[:10]))

    vectorizer.fit(raw_names)

    with open(os.path.join('clustering_resources', 'city_name_model.pkl'), 'wb') as f:
        pickle.dump(vectorizer, f)


if __name__ == "__main__":
    app.run(main)