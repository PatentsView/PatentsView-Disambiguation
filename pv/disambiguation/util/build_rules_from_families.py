import collections
import gzip
import pickle

from absl import app
from absl import flags
from absl import logging
from tqdm import tqdm

logging.set_verbosity(logging.INFO)

FLAGS = flags.FLAGS

flags.DEFINE_string('family_csv', '/iesl/canvas/nmonath/data/patentfamily/patstat_patent_docdb_inpadoc.csv.gz', '')
flags.DEFINE_string('output', 'data/inventor/', '')
flags.DEFINE_string('record_pickle', '/iesl/data/patentsview/2020-06-10/rawinventor.tsv.pkl', '')

logging.set_verbosity(logging.INFO)


def grab_by_id(csvfilename):
    docdb2patents = collections.defaultdict(list)
    inpadoc2patents = collections.defaultdict(list)
    with gzip.open(csvfilename, 'rt') as fin:
        for idx, line in enumerate(fin):
            logging.log_every_n(logging.INFO, 'Read %s lines of %s', 1000, idx, csvfilename)
            if idx == 0:
                continue
            splt = line.strip().split(',')
            docdb = splt[3]
            inpadoc = splt[4]
            pat_no = splt[1]
            docdb2patents[docdb].append(pat_no)
            inpadoc2patents[inpadoc].append(pat_no)
    logging.info('Finished loading csv file!')
    return docdb2patents, inpadoc2patents


def grab_names(fam2patent, record_pkl):
    def get_inv_for_patent(patno):
        res = []
        for i in range(50):
            mid = '%s-%s' % (patno, i)
            if mid in record_pkl:
                res.append(record_pkl[mid])
            else:
                break
        return res

    name2mentions = collections.defaultdict(set)
    for fam, patents in tqdm(fam2patent.items(), desc='fam2patent'):
        for patno in patents:
            mentions = get_inv_for_patent(patno)
            for inventormention in mentions:
                fn = inventormention.first_name()[0] if len(
                    inventormention.first_name()) > 0 else inventormention.mention_id
                ln = inventormention.last_name()[0] if len(
                    inventormention.last_name()) > 0 else inventormention.mention_id
                mname = fn + "-" + ln + '-' + str(fam)
                name2mentions[mname].add(patno)
    return name2mentions


def main(argv):
    docdb2patents, inpadoc2patents = grab_by_id(FLAGS.family_csv)
    with open(FLAGS.record_pickle, 'rb') as fin:
        record_pkl = pickle.load(fin)
    name2mentions = grab_names(docdb2patents, record_pkl)
    with open(FLAGS.output + '/docdb.pkl', 'wb') as fout:
        pickle.dump(name2mentions, fout)

    name2mentions = grab_names(inpadoc2patents, record_pkl)
    with open(FLAGS.output + '/inpadoc.pkl', 'wb') as fout:
        pickle.dump(name2mentions, fout)


if __name__ == "__main__":
    app.run(main)
