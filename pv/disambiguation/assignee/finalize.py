import collections
import numpy as np
import os
import pickle
from absl import app
from absl import flags
from absl import logging
from tqdm import tqdm
import pandas as pd
logging.set_verbosity(logging.INFO)
# from experiments.assignee_Z_Frame_analysis import plot_Z_v_text_distance


def process_file(point2clusters, clusters, pkl_file):
    res = pickle.load(open(pkl_file, 'rb'))
    num_assign = 0
    for c, res_item in res.items():
        logging.log_first_n(logging.INFO, 'canopy %s', 5, c)
        for idx in range(len(res_item[0])):
            if res_item[1][idx] not in clusters:
                logging.log_every_n_seconds(logging.INFO, 'new cluster %s -> %s', 1, res_item[1][idx], len(clusters))
                clusters[res_item[1][idx]] = len(clusters)
            point2clusters[res_item[0][idx]].append(clusters[res_item[1][idx]])
            logging.log_every_n_seconds(logging.INFO, 'points %s -> %s', 1, res_item[0][idx], clusters[res_item[1][idx]])
            num_assign += 1
    return num_assign


def process(point2clusters, clusters, rundir):
    num_assign = 0
    for f in tqdm([f for f in os.listdir(rundir) if f.endswith('.pkl') and 'internals' not in f and 'job' in f]):
        num_assign += process_file(point2clusters, clusters, os.path.join(rundir, f))
    return num_assign

def check_assignee_disambiguation_tsv(output_file):
    # d = pd.read_csv("disambiguation.tsv", sep="\t", names=['id', 'ass_id'])
    d = pd.read_csv(output_file, sep="\t", names=['id', 'ass_id'])
    unique_ids = len(list(d['id']))
    unique_assignee_ids = len(set(list(d['ass_id'])))
    print(f"There are {unique_ids} unique IDs and {unique_assignee_ids} unique_assignee_ids")
    if unique_ids < 11000000 or unique_assignee_ids < 450000 or unique_assignee_ids > 700000:
        raise Exception(f"ASSIGNEE DISAMBIGUATION RESULTS LOOK WRONG")
    s = d.groupby('ass_id', sort=True).count()
    f = s.sort_values(by='id', ascending=False).head(20)
    f = f.reset_index()
    if f['id'][0] > 350000:
        print(f)
        raise Exception(f"ASSIGNEE DISAMBIGUATION OVER-CLUSTERED")
    if f['id'][0] < 50000:
        print(f)
        raise Exception(f"ASSIGNEE DISAMBIGUATION UNDER-CLUSTERED")


def finalize_results(config):
    point2clusters = collections.defaultdict(list)
    cluster_dict = dict()
    logging.info('loading canopy results..')
    total_num_assignments = process(point2clusters, cluster_dict, config['assignee']['clustering_output_folder'])
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
        overall_idx += len(clusters)
        pid2idx[pid] = idx
    from scipy.sparse import coo_matrix
    mat = coo_matrix((data, (row, col)), shape=(len(cluster_dict) + len(point2clusters), len(cluster_dict) + len(point2clusters)))
    from scipy.sparse.csgraph import connected_components
    logging.info('running cc...')
    n_cc, lbl_cc = connected_components(mat, directed=True, connection='weak')
    logging.info('running cc...done')

    logging.info('loading mentions...')
    end_date = config["DATES"]["END_DATE"]
    path = f"{config['BASE_PATH']['assignee']}".format(end_date=end_date) + config['BUILD_ASSIGNEE_NAME_MENTIONS'][
        'feature_out']
    print(path)
    with open(path + '.%s.pkl' % 'records', 'rb') as fin:
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
                                'we didnt do any more disambiguation for %s', 10, m.uuid)
            for rid in m.mention_ids:
                missing_mid2eid[rid] = m.uuid
    output_file = "{path}/disambiguation.tsv".format(path=config['assignee']['clustering_output_folder'])
    logging.info(f'writing output to {output_file}...')
    if os.path.isfile(output_file):
        print("Removing Current File in Directory")
        os.remove(output_file)
    if os.path.isfile(output_file):
        print("Removing Current File in Directory")
        os.remove(output_file)
    with open(output_file, 'w') as fout:
        for m, e in mid2eid.items():
            fout.write('%s\t%s\n' % (m, e))
        for m, e in missing_mid2eid.items():
            if m not in mid2eid:
                fout.write('%s\t%s\n' % (m, e))
    check_assignee_disambiguation_tsv(output_file)
    logging.info('writing output ... done.')


def main(argv):
    import configparser
    config = configparser.ConfigParser()
    config.read(['config/database_config.ini', 'config/database_tables.ini', 'config/assignee/run_clustering.ini'])
    finalize_results(config)


# import pdb
# pdb.set_trace()


# import pdb
# pdb.set_trace()


if __name__ == "__main__":
    app.run(main)
