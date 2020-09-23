import collections
import os
import pickle

import numpy as np
from absl import app
from absl import flags
from absl import logging
from tqdm import tqdm

FLAGS = flags.FLAGS

flags.DEFINE_string('input', 'exp_out/assignee/run_24', '')
flags.DEFINE_string('assignee_name_mentions', 'data/assignee/assignee_mentions.records.pkl', '')

flags.DEFINE_string('output', 'exp_out/assignee/run_24/disambiguation.tsv', '')

logging.set_verbosity(logging.INFO)


def process_file(point2clusters, clusters, pkl_file):
    res = pickle.load(open(pkl_file, 'rb'))
    num_assign = 0
    for c, res in res.items():
        logging.log_first_n(logging.INFO, 'canopy %s', 5, c)
        for idx in range(len(res[0])):
            if res[1][idx] not in clusters:
                logging.log_every_n_seconds(logging.INFO, 'new cluster %s -> %s', 1, res[1][idx], len(clusters))
                clusters[res[1][idx]] = len(clusters)
            point2clusters[res[0][idx]].append(clusters[res[1][idx]])
            logging.log_every_n_seconds(logging.INFO, 'points %s -> %s', 1, res[0][idx], clusters[res[1][idx]])
            num_assign += 1
    return num_assign


def process(point2clusters, clusters, rundir):
    num_assign = 0
    for f in tqdm([f for f in os.listdir(rundir) if f.endswith('.pkl')]):
        num_assign += process_file(point2clusters, clusters, os.path.join(rundir, f))
    return num_assign


def main(argv):
    point2clusters = collections.defaultdict(list)
    cluster_dict = dict()
    logging.info('loading canopy results..')
    total_num_assignments = process(point2clusters, cluster_dict, FLAGS.input)
    logging.info('total_num_clusters %s', len(cluster_dict))
    logging.info('total_num_assignments %s', total_num_assignments)
    logging.info('loading canopy results...done')

    row = np.ones(total_num_assignments, np.int64)
    col = np.ones(total_num_assignments, np.int64)
    data = np.ones(total_num_assignments, np.int64)
    overall_idx = 0
    pid2idx = dict()
    for idx, (pid, clusters) in tqdm(enumerate(point2clusters.items()), 'building sparse graph'):
        row[overall_idx:overall_idx + len(clusters)] *= idx
        col[overall_idx:overall_idx + len(clusters)] = np.array(clusters, dtype=np.int64) + len(point2clusters)
        # import pdb
        # pdb.set_trace()
        overall_idx += len(clusters)
        pid2idx[pid] = idx

    # import pdb
    # pdb.set_trace()
    from scipy.sparse import coo_matrix
    mat = coo_matrix((data, (row, col)),
                     shape=(len(cluster_dict) + len(point2clusters), len(cluster_dict) + len(point2clusters)))
    from scipy.sparse.csgraph import connected_components
    logging.info('running cc...')
    n_cc, lbl_cc = connected_components(mat, directed=True, connection='weak')
    logging.info('running cc...done')

    # import pdb
    # pdb.set_trace()

    logging.info('loading mentions...')
    with open(FLAGS.assignee_name_mentions, 'rb') as fin:
        assignee_mentions = pickle.load(fin)

    import uuid
    final_uuids = [str(uuid.uuid4()) for _ in range(n_cc)]
    mid2eid = dict()
    missing_mid2eid = dict()
    for amid, m in tqdm(assignee_mentions.items(), 'assigning ids'):
        if m.uuid in pid2idx:
            for rid in m.mention_ids:
                logging.log_every_n_seconds(logging.INFO,
                                            'mention: %s -> entity %s', 1,
                                            rid, final_uuids[lbl_cc[pid2idx[m.uuid]]])
                mid2eid[rid] = final_uuids[lbl_cc[pid2idx[m.uuid]]]
        else:
            logging.log_first_n(logging.INFO,
                                'we didnt do any more diambiguation for %s', 100, m.uuid)
            for rid in m.mention_ids:
                missing_mid2eid[rid] = m.uuid
    logging.info('writing output ...')
    with open(FLAGS.output, 'w') as fout:
        for m, e in mid2eid.items():
            fout.write('%s\t%s\n' % (m, e))
        for m, e in missing_mid2eid.items():
            if m not in mid2eid:
                fout.write('%s\t%s\n' % (m, e))
    logging.info('writing output ... done.')


if __name__ == "__main__":
    app.run(main)
