import collections

import matplotlib.pyplot as plt
import numpy as np
from absl import app
from absl import flags
from tqdm import tqdm

FLAGS = flags.FLAGS

flags.DEFINE_string('old_file', '', 'old disambiguation file')
flags.DEFINE_string('new_file', '', 'new disambiguation file')


def load_disambiguation(filename):
    record_ids = []
    cluster_ids = []
    with open(filename) as fin:
        for line in tqdm(fin):
            splt = line.strip().split('\t')
            if len(splt) == 2:
                record_ids.append(splt[0])
                cluster_ids.append(splt[1])
            else:
                print('malformed line %s' % line)
    return record_ids, cluster_ids


def cluster_size_counts(record_ids, predicted_entity_ids):
    e2pts = collections.defaultdict(list)
    e2count = collections.defaultdict(int)
    for rid, eid in tqdm(zip(record_ids, predicted_entity_ids), 'counting', len(record_ids)):
        e2pts[eid].append(rid)
        e2count[eid] += 1
    return e2count, e2pts


def cluster_size_hist(old_e2count, new_e2count, n_bins=10000):
    x1 = np.array([x for x in old_e2count.values()])
    x2 = np.array([x for x in new_e2count.values()])
    hist_data = [x1, x2]
    group_labels = ['Old Disambiguation', 'New Disambiguation']
    colors = ['#2D8DDB', '#6307DB']
    bins = np.linspace(0, 25000, n_bins)

    plt.hist(hist_data, bins, label=group_labels, color=colors, log=True)
    plt.legend(loc='upper right')
    plt.savefig('assignee.hist.pdf')


def stats(x, name):
    print('%s mean %s' % (name, np.mean(x)))
    print('%s median %s' % (name, np.median(x)))
    print('%s max %s' % (name, np.max(x)))
    print('%s min %s' % (name, np.min(x)))


def cluster_size_basic_stats(old_e2count, new_e2count):
    x1 = np.array([x for x in old_e2count.values()])
    x2 = np.array([x for x in new_e2count.values()])
    stats(x1, 'old')
    stats(x2, 'new')


def main(argv):
    old_record_ids, old_cluster_ids = load_disambiguation(FLAGS.old_file)
    new_record_ids, new_cluster_ids = load_disambiguation(FLAGS.new_file)

    new_e2count, new_e2pts = cluster_size_counts(new_record_ids, new_cluster_ids)
    old_e2count, old_e2pts = cluster_size_counts(old_record_ids, old_cluster_ids)

    cluster_size_basic_stats(old_e2count, new_e2count)

    cluster_size_hist(old_e2count, new_e2count)


if __name__ == "__main__":
    app.run(main)
