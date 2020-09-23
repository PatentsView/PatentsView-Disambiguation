from absl import app

def load_all_evaluation_ids(eval_files):
    ids = set()
    for fn in eval_files:
        with open(fn) as fin:
            for line in fin:
                splt = line.split('\t')
                ids.add(splt[0])
    return ids

def write_fixed_training_data(infile, outfile, eval_ids):
    """Remove the eval ids from the training data.

    :param infile:
    :param outfile:
    :param eval_ids:
    :return:
    """
    num_lines_total = 0
    num_in_eval = 0
    with open(infile) as fin:
        with open(outfile, 'w') as fout:
            for line in fin:
                splt = line.strip().split('\t')
                num_lines_total += 1
                if splt[0] not in eval_ids:
                    fout.write(line)
                else:
                    num_in_eval += 1
    print('%s | num records %s | num in eval %s ' % (infile, num_lines_total, num_in_eval))

def main(argv):
    eval_cc_in = '/iesl/canvas/nmonath/research/entity-resolution/er/data/inventor-train/eval_common_characteristics.txt.orig'
    eval_cc_out = '/iesl/canvas/nmonath/research/entity-resolution/er/data/inventor-train/eval_common_characteristics.train'
    eval_mixture_in = '/iesl/canvas/nmonath/research/entity-resolution/er/data/inventor-train/eval_mixture.txt.org'
    eval_mixture_out = '/iesl/canvas/nmonath/research/entity-resolution/er/data/inventor-train/eval_mixture.train'
    eval_fns = ['/iesl/canvas/nmonath/research/entity-resolution/er/data/evaluation-data/inventor/2015_workshop/eval_als_common.txt',
                '/iesl/canvas/nmonath/research/entity-resolution/er/data/evaluation-data/inventor/2015_workshop/eval_als.txt',
                '/iesl/canvas/nmonath/research/entity-resolution/er/data/evaluation-data/inventor/2015_workshop/eval_ens.txt',
                '/iesl/canvas/nmonath/research/entity-resolution/er/data/evaluation-data/inventor/2015_workshop/eval_is.txt',
                '/iesl/canvas/nmonath/research/entity-resolution/er/data/evaluation-data/inventor/handlabeled/gold-labels.txt'
                ]
    eval_ids = load_all_evaluation_ids(eval_fns)
    write_fixed_training_data(eval_cc_in, eval_cc_out, eval_ids)
    write_fixed_training_data(eval_mixture_in, eval_mixture_out, eval_ids)

if __name__ == "__main__":
    app.run(main)