import os

from tqdm import tqdm

import os
from absl import flags
from absl import app
from absl import logging

import pickle
import collections
import numpy as np

FLAGS = flags.FLAGS

flags.DEFINE_string('results_to_map', '', '')

logging.set_verbosity(logging.INFO)


def uuid2mid_map(rawassignee_file):
    res = dict()
    res2assigneeid = dict()
    str2assigneeid = dict()
    mid2str = dict()
    with open(rawassignee_file, 'r') as fin:
        # uuid    patent_id       assignee_id     rawlocation_id  type    name_first      name_last       organization    sequence
        for line in fin:
            splt = line.strip().split('\t')
            res[splt[0]] = '%s-%s' % (splt[1], splt[-1])
            res2assigneeid['%s-%s' % (splt[1], splt[-1])] = splt[2]
            mid2str['%s-%s' % (splt[1], splt[-1])] = '%s-%s-%s' % (splt[5], splt[6], splt[7])
            str2assigneeid['%s-%s-%s' % (splt[5], splt[6], splt[7])] = splt[2]
    return res, res2assigneeid, mid2str

def main(argv):
    idmap, results, mid2str = uuid2mid_map('/iesl/data/patentsview/2019-08-01/rawassignee.tsv')
    files = [
        # 'data/evaluation-data/assignee/nber-subset/nber-subset.test',
        #         'data/evaluation-data/assignee/nber-subset/nber-subset.dev',
        #         'data/evaluation-data/assignee/nber-subset/nber-subset.tsv',
        #         'data/evaluation-data/assignee/nber/nber.tsv',
        #         'data/evaluation-data/assignee/nber/nber.dev',
        #         'data/evaluation-data/assignee/nber/nber.test',
                'data/evaluation-data/assignee/air-umass/air_umass.dev',
                'data/evaluation-data/assignee/air-umass/air_umass.tsv',
                'data/evaluation-data/assignee/air-umass/air_umass.test',
            'data/evaluation-data/assignee/rawassignee.attrb']
    basedir = '/iesl/canvas/nmonath/research/clustering/inv-dis-internal3/'

    base_out_dir = 'data/eval-assignee'
    os.makedirs(base_out_dir,exist_ok=True)

    for f in files:
        logging.info('processing file %s', f)
        with open(os.path.join(base_out_dir, os.path.basename(f)), 'w') as fout:
            with open(os.path.join(basedir, f), 'r') as fin:
                for line in fin:
                    splt = line.split('\t')
                    if splt[0] not in idmap:
                        logging.warning('missing id! %s', splt[0])
                    else:
                        splt[0] = idmap[splt[0]]
                        fout.write('\t'.join(splt))

    for f in files:
        if not f.endswith('attrb'):
            logging.info('processing file %s', f)
            with open(os.path.join(base_out_dir, os.path.basename(f)) + '.str', 'w') as fout:
                with open(os.path.join(basedir, f), 'r') as fin:
                    printed = set()
                    for line in fin:
                        splt = line.split('\t')
                        if splt[0] not in idmap:
                            logging.warning('missing id! %s', splt[0])
                        else:
                            splt[0] = idmap[splt[0]]
                            if mid2str[splt[0]] not in printed:
                                splt[0] = mid2str[splt[0]]
                                fout.write('\t'.join(splt))
                                printed.add(splt[0])


    with open(os.path.join(base_out_dir, 'pv-dis.tsv'), 'w') as fout:
        for k,v in results.items():
            fout.write('%s\t%s\n' % (k,v))
    with open(os.path.join(base_out_dir, 'pv-dis-str.tsv'), 'w') as fout:
        printed = set()
        for k,v in results.items():
            if mid2str[k] not in printed:
                fout.write('%s\t%s\n' % (mid2str[k], v))
                printed.add(mid2str[k])

    if FLAGS.results_to_map != '':
        with open(FLAGS.results_to_map, 'r') as fin:
            with open(os.path.join(base_out_dir, 'other-dis-str.tsv'), 'w') as fout:
                printed = set()
                for line in fin:
                    splt = line.strip().split('\t')
                    k = splt[0]
                    v = splt[1]
                    if k in mid2str and mid2str[k] not in printed:
                        fout.write('%s\t%s\n' % (mid2str[k], v))
                        printed.add(mid2str[k])



if __name__ == '__main__':
    app.run(main)
