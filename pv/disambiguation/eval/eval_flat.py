
from absl import flags
from absl import app
from absl import logging
from tqdm import tqdm
FLAGS = flags.FLAGS
from grinch.eval_pw_f1 import eval_micro_pw_f1


flags.DEFINE_string('pred', '', 'data path')
flags.DEFINE_string('gold', '', 'data path')
flags.DEFINE_string('exp_name', 'eval_flat', 'data path')
flags.DEFINE_boolean('skip_missing', False, '')
flags.DEFINE_string('outfile', '', 'data path')


def load_tsv(filename, to_keep):
    pids = []
    lbls = []
    with open(filename, 'r') as fin:
        for line in fin:
            splt = line.strip().split('\t')
            if not to_keep or splt[0] in to_keep:
                pids.append(splt[0])
                lbls.append(splt[1])
    return pids,lbls


def main(argv):
    logging.info('Running eval - %s ', str(argv))
    # wandb.init(project="%s" % (FLAGS.exp_name))
    # wandb.config.update(flags.FLAGS)
    logging.info('loading gold %s', FLAGS.gold)
    gold_pids, gold_lbls = load_tsv(FLAGS.gold, None)
    gold_pids_set = set(gold_pids)
    logging.info('number of gold points %s', len(gold_pids))
    logging.info('loading pred %s', FLAGS.pred)
    pred_pids, pred_lbls = load_tsv(FLAGS.pred, gold_pids_set)
    logging.info('number of pred points %s', len(pred_pids))
    missing = gold_pids_set.difference(set(pred_pids))
    if len(gold_pids) != len(pred_pids):
        for idx, v in enumerate(missing):
            if idx > 5:
                break
            logging.info('missing %s', v)
        logging.info('and more....')
    if FLAGS.skip_missing:
        intersection = gold_pids_set.intersection(set(pred_pids))
        idx = sorted([x for x in range(len(gold_pids))], key=lambda x: gold_pids[x])
        sorted_pids = [gold_pids[x] for x in idx if gold_pids[x] in intersection]
        sorted_gold = [gold_lbls[x] for x in idx if gold_pids[x] in intersection]
        idx = sorted([x for x in range(len(pred_pids))], key=lambda x: pred_pids[x])
        sorted_pred = [pred_lbls[x] for x in idx if pred_pids[x] in intersection]
        if FLAGS.outfile != '':
            with open(FLAGS.outfile + '.missing', 'w') as fout:
                for m in missing:
                    fout.write('%s\n' % m)
        metrics, in_both, just_pred, just_gold = eval_micro_pw_f1(sorted_pred, sorted_gold, return_pairs=True)
        # wandb.log(metrics)
        for k, v in metrics.items():
            logging.info('%s: %s', k, v)

    else:
        assert len(set(pred_pids).intersection(gold_pids_set)) == len(gold_pids_set), 'gold and pred are not the same points!'
        idx = sorted([x for x in range(len(gold_pids))], key=lambda x: gold_pids[x])
        sorted_pids = [gold_pids[x] for x in idx]
        sorted_gold = [gold_lbls[x] for x in idx]
        idx = sorted([x for x in range(len(pred_pids))], key=lambda x: pred_pids[x])
        sorted_pred = [pred_lbls[x] for x in idx]
        metrics, in_both, just_pred, just_gold = eval_micro_pw_f1(sorted_pred, sorted_gold, return_pairs=True)
        # wandb.log(metrics)
        for k,v in metrics.items():
            logging.info('%s: %s', k, v)

    if FLAGS.outfile != '':
        with open(FLAGS.outfile, 'w') as fout:
            for idx in range(len(sorted_pred)):
                fout.write('%s\t%s\t%s\n' % (sorted_pids[idx], sorted_pred[idx], sorted_gold[idx]))



if __name__ == '__main__':
    app.run(main)




