
import os
import time
from nltk.corpus import stopwords

from absl import logging
from nltk import sent_tokenize
from nltk import word_tokenize
from absl import flags
from absl import app

FLAGS = flags.FLAGS

# flags.DEFINE_string('infile', '/iesl/data/patentsview/2019-12-31/brf_sum_text.tsv', 'data path')
# flags.DEFINE_string('output_dir', '/iesl/data/patentsview/2019-12-31/brf_sum_text_sents', 'data path')

flags.DEFINE_string('infile', '/iesl/data/patentsview/2019-12-31/patent.tsv', 'data path')
flags.DEFINE_string('output_dir', '/iesl/data/patentsview/2019-12-31/patent_sents', 'data path')

flags.DEFINE_integer('chunk_id', 0, 'number of chunks')
flags.DEFINE_integer('chunk_size', 100000, 'number of documents per chunk')

logging.set_verbosity(logging.INFO)


stop_words = set(stopwords.words('english'))

def clean_text(text):
    if text and text[0] == '"':
        text = text[1:]
    if text and text[-1] == '"':
        text = text[:-1]
    text = text.replace('/', ' / ')
    text = text.replace('.-', ' .- ')
    text = text.replace('.', ' . ')
    text = text.replace('\'', ' \' ')
    text = text.lower()
    return text


def find_start_of_chunk(fin, first_row_of_chunk):
    if first_row_of_chunk == 0:
        return fin
    else:
        for idx, _ in enumerate(fin):
            if idx + 1 == first_row_of_chunk:
                return fin

def process_chunk_brf_summary_text(infile, outfile, first_row_of_chunk, chunk_size):
    with open(outfile,'w') as fout:
        f = open(infile)
        next(f)  # skip header
        find_start_of_chunk(f,first_row_of_chunk)
        st = time.time()
        for idx, line in enumerate(f):
            logging.info('Processed %s of %s lines | %s elapsed | %s s/line', idx, chunk_size, time.time() - st,
                         (time.time() - st) / max(idx, 1))
            splt = line.strip().split('\t')
            if len(splt) == 3:
                sents = sent_tokenize(splt[2])
                for s in sents:
                    s = s.strip()
                    if s:
                        fout.write('%s\n' % clean_text(s))
            else:
                logging.error('Malformed line %s', line)
            if idx + 1 == chunk_size:
                break

def process_chunk_patent(infile, outfile, first_row_of_chunk, chunk_size):
    with open(outfile,'w') as fout:
        f = open(infile)
        next(f)  # skip header
        find_start_of_chunk(f,first_row_of_chunk)
        st = time.time()
        for idx,line in enumerate(f):
            logging.info('Processed %s of %s lines | %s elapsed | %s s/line', idx, chunk_size, time.time() - st, (time.time() - st) / max(idx,1))
            splt = line.strip().split('\t')
            sents = sent_tokenize(splt[5])
            title = splt[6].strip()
            if title:
                fout.write('%s\n' % title)
            for s in sents:
                s = s.strip()
                if s:
                    fout.write('%s\n' % clean_text(s))
            if idx + 1 == chunk_size:
                break


def main(argv):
    os.makedirs(FLAGS.output_dir, exist_ok=True)

    if 'brf_sum_text' in FLAGS.infile:
        process_chunk_brf_summary_text(FLAGS.infile, '%s/chunk_%s.txt' % (FLAGS.output_dir, FLAGS.chunk_id),
                                       FLAGS.chunk_id*FLAGS.chunk_size, FLAGS.chunk_size)
    else:
        process_chunk_patent(FLAGS.infile, '%s/chunk_%s.txt' % (FLAGS.output_dir, FLAGS.chunk_id),
                                       FLAGS.chunk_id * FLAGS.chunk_size, FLAGS.chunk_size)




if __name__ == "__main__":
    app.run(main)