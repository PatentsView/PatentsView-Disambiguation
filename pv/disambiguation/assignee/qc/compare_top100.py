import numpy as np

from scipy.optimize import linear_sum_assignment
import sys

def compute_pairwise_sim(list1, list2):
    sims = np.zeros((len(list1) + len(list2), (len(list1) + len(list2))), dtype=np.int32)
    for idx1, i in enumerate(list1):
        for idx2, j in enumerate(list2):
            sims[idx1, len(list1) + idx2] = len(i.intersection(j))
            sims[len(list1) + idx2, idx1] = len(i.intersection(j))

    costs = np.max(sims) - sims
    max_sim = np.max(sims)
    res = linear_sum_assignment(costs)
    picks = [None for _ in list1]
    scores = [None for _ in list1]
    # import pdb
    # pdb.set_trace()
    for idx in range(len(list1)):
        i = res[0][idx]
        j = res[1][idx]
        scores[i] = costs[i,j]
        picks[i] = j - len(list1) if scores[i] != max_sim else "Missing"
    return picks,scores

def load_file(filename):
    res = []
    with open(filename, 'r') as fin:
        for line in fin:
            splt = line.split('\t')[-1].split(';')
            res.append(set(splt))
    return res


def report(newlist, oldlist, outfile):
    alignment, alignment_score = compute_pairwise_sim(newlist,oldlist)
    with open(outfile, 'w') as fout:
        for idx in range(len(newlist)):
            fout.write("%s\t%s\t%s\n" % (idx, alignment[idx], newlist[idx]))


if __name__ == "__main__":
    newlist = load_file(sys.argv[1])
    oldlist = load_file(sys.argv[2])
    report(newlist, oldlist, sys.argv[3])