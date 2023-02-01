import pandas as pd
import re, editdistance, json


def load_assignee_analyzer_configuration(assignee_abbreviation_file, assignee_correction_file,
                                         assignee_stopphrase_file):
    """
    Load three preprocessing files: stop phrases, words that need to be corrected & abbreviations that need to be expanded
    :param assignee_abbreviation_file: File containing abbreviations
    :param assignee_correction_file:
    :param assignee_stopphrase_file:
    :return:
    """
    import json
    correction_configuration = list()
    with open(assignee_correction_file) as fin:
        for line in fin:
            correction_configuration.append(line.strip())
    stopphrase_configuration = list()
    with open(assignee_stopphrase_file) as fin:
        for line in fin:
            stopphrase_configuration.append(line.strip())
    remapping_configuration = json.load(open(assignee_abbreviation_file, 'r'))
    return correction_configuration, stopphrase_configuration, remapping_configuration


def char_wb_ngram_with_lower_priority_exclusion(text_document, ngram_range, white_spaces,
                                                exclusion_list=None):
    """Whitespace sensitive char-n-gram tokenization that excludes certain words.
    Tokenize text_document into a sequence of character n-grams
    operating only inside word boundaries. n-grams at the edges
    of words are padded with space. Words in exlusion list are not 'ngramed'. Derived from https://github.com/scikit-learn/scikit-learn/blob/36958fb240fbe435673a9e3c52e769f01f36bec0/sklearn/feature_extraction/text.py#L293"""
    # normalize white spaces
    text_document = white_spaces.sub(" ", text_document)

    min_n, max_n = ngram_range
    ngrams = []
    if exclusion_list is None:
        exclusion_list = []
    # bind method outside of loop to reduce overhead
    ngrams_append = ngrams.append

    for w in text_document.split():
        if w not in exclusion_list:
            # if len(w) > lower_char_limit:
            w = " " + w + " "
            w_len = len(w)
            for n in range(min_n, max_n + 1):
                offset = 0
                ngrams_append(w[offset: offset + n])
                while offset + n < w_len:
                    offset += 1
                    ngrams_append(w[offset: offset + n])
                if offset == 0:  # count a short word (w_len < n) only once
                    break
        else:
            ngrams_append(w)
    return ngrams


assignee_abbreviation_file = 'clustering_resources/assignee_abbreviations.json'
assignee_correction_file = 'clustering_resources/assignee_corrections.txt'
assignee_stopphrase_file = 'clustering_resources/assignee_stopwords.txt'
# Maximun distance threshold for "correcting" words.
THRESHOLD = 2
# Ngrams to generate
N_GRAM_RANGE = (2, 10)
CORRECTION_CONFIGURATION, STOPPHRASE_CONFIGURATION, REMAPPING_CONFIGURATION = load_assignee_analyzer_configuration(
    assignee_abbreviation_file, assignee_correction_file, assignee_stopphrase_file)


# For large datasets, use paralllization to pickle processed data to output file
def generate_pickle_data(name_subset, csv_queue):
    for name in name_subset:
        csv_queue.put(analyze_assignee_name(name))


def remove_stopphrase(doc):
    import re
    for stopphrase in STOPPHRASE_CONFIGURATION:
        doc = re.sub(r"\b" + stopphrase + r"\b", " ", doc).strip()
    return doc


def correct_tokens(token):
    import editdistance
    if len(token) > 1.5 * THRESHOLD:
        for primary_word in CORRECTION_CONFIGURATION:
            how_different = editdistance.distance(primary_word.lower(),
                                                  token.lower())
            if how_different == 0:
                break
            # Difference is less than threshold, but there is some difference
            if THRESHOLD >= how_different > 0:
                token = primary_word
                # Order in which suffixes are specified matters
                break
    return re.sub("(\s){2,}", "\\1", token.lower().strip())


def expand_abbreviation(doc):
    processed_document_elements = []
    for token in doc.split():
        for abbreviation, replacement in REMAPPING_CONFIGURATION.items():
            # Check if any of the abbreviations are used
            abbreviation_pattern = abbreviation + "[,.\s]*$"
            if re.match(abbreviation_pattern, token, re.IGNORECASE):
                token = replacement.split()
                break
        if isinstance(token, list):
            processed_document_elements = processed_document_elements + [correct_tokens(x) for x in token]
        else:
            processed_document_elements.append(correct_tokens(token))
    return " ".join(processed_document_elements)


def analyze_assignee_name(assignee_name, *args, **kwargs):
    assignee_name = assignee_name.translate({ord(c): " " for c in "!@#$%^&*()[]{};:,./<>?\|`~-=_+\"'"})
    import unicodedata
    assignee_name = unicodedata.normalize('NFD', assignee_name)
    assignee_name = expand_abbreviation(assignee_name)
    assignee_name = remove_stopphrase(assignee_name)
    return char_wb_ngram_with_lower_priority_exclusion(assignee_name, exclusion_list=CORRECTION_CONFIGURATION,
                                                       ngram_range=kwargs.get('ngram_range', N_GRAM_RANGE),
                                                       white_spaces=kwargs.get('white_spaces', re.compile(r"\s\s+")))


# Multiprocessing output writer
def mp_pickle_writer(write_queue, target_file):
    import pickle
    with open(target_file, 'wb') as write_file:
        pickler = pickle.Pickler(write_file)
        while 1:
            message_data = write_queue.get()
            if not isinstance(message_data, list):
                # "kill" is the special message to stop listening for messages
                if message_data == 'kill':
                    break
                else:
                    print(message_data)
                    raise Exception("Problem: {message}".format(message=message_data))
            pickler.dump(message_data)


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


if __name__ == '__main__':
    rawassignee = pd.read_csv("patent_rawassignee_20220729.csv", sep=",")
    from tqdm import tqdm

    organization_names = [x for x in tqdm(rawassignee.organization.tolist()) if not pd.isnull(x)]
    person_names = [x for x in tqdm((rawassignee.name_first + ' ' + rawassignee.name_last).tolist()) if
                    not pd.isnull(x)]
    # Create list of names
    names = organization_names + person_names
    import multiprocessing as mp

    # Used on disambiguation server which can support high multiprocessing
    pool = mp.Pool(40)
    # Output file name
    cpc_file = "processed_names.pkl"
    manager = mp.Manager()
    # Queue to which processed data is written. Also the queue from which output writer gets data to write to file
    csv_queue = manager.Queue()
    writer = pool.apply_async(mp_pickle_writer, (
        csv_queue,
        cpc_file))
    p_list = []
    for name_subset in tqdm(chunks(names, 40), desc='Queuing'):
        p = pool.apply_async(generate_pickle_data, (name_subset, csv_queue,))
        p_list.append(p)

    for t in tqdm(p_list, desc='de-Queueing'):
        t.get()

    csv_queue.put('kill')
    writer.get()
    pool.close()
    pool.join()
