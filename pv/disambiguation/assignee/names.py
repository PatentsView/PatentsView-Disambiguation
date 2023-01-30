import re
import string
import unicodedata

from absl import logging
from nltk import word_tokenize

import json
import editdistance
import re
import uuid
import configparser

last_split_patterns = ['(?i)as .* by .* of', '(?i)on behalf of', '(?i)as .* by', '(?i)c/o', '(?i)board of regents of']
first_split_patterns = ['(?i)also trading as', '(?i)acting by and through']


class AssigneePreprocessor:
    def __init__(self, assignee_abbreviation_file, assignee_correction_file, assignee_stopphrase_file, threshold=0.2):
        self.correction_configuration = list()
        with open(assignee_correction_file) as fin:
            for line in fin:
                self.correction_configuration.append(line.strip())
        self.stopphrase_configuration = list()
        with open(assignee_stopphrase_file) as fin:
            for line in fin:
                self.stopphrase_configuration.append(line.strip())
        self.remapping_configuration = json.load(open(assignee_abbreviation_file, 'r'))
        # self.assignee_suffixes  = remapping_configuration.keys()
        self.threshold = threshold

    def preprocess(self, doc):
        processed_doc = self.expand_abbreviation(doc)
        processed_doc = self.correct_tokens(processed_doc)
        processed_doc = self.remove_stopphrase(processed_doc)
        return processed_doc

    def remove_stopphrase(self, doc):
        for stopphrase in self.stopphrase_configuration:
            doc = re.sub(r"\b" + stopphrase + r"\b", " ", doc).strip()
        return doc

    def correct_tokens(self, doc):
        processed_document_elements = []
        for token in doc.split():
            if len(token) > 1.5 * self.threshold:
                for primary_word in self.correction_configuration:
                    how_different = editdistance.distance(primary_word.lower(),
                                                          token.lower())
                    if how_different == 0:
                        break
                    # Difference is less than threshold, but there is some difference
                    if self.threshold >= how_different > 0:
                        token = primary_word
                        # Order in which suffixes are specified matters
                        break
            processed_document_elements.append(token.lower().strip())
        return " ".join(processed_document_elements)

    def expand_abbreviation(self, doc):
        processed_document_elements = []
        for token in doc.split():
            for abbreviation, replacement in self.remapping_configuration.items():
                # Check if any of the abbreviations are used
                abbreviation_pattern = abbreviation + "[,.\s]*$"
                if re.match(abbreviation_pattern, token, re.IGNORECASE):
                    token = replacement.split()
                    break
            if isinstance(token, list):
                processed_document_elements = processed_document_elements + token
            else:
                processed_document_elements.append(token)
        return " ".join(processed_document_elements)


c = configparser.ConfigParser()
c.read("config.ini")
project_root = c['FOLDERS']['data_root']
assignee_preprocessor = AssigneePreprocessor(
    assignee_abbreviation_file=f'{project_root}/clustering_resources/assignee_abbreviations.json',
    assignee_correction_file=f'{project_root}/clustering_resources/assignee_corrections.txt',
    assignee_stopphrase_file=f'{project_root}/clustering_resources/assignee_stopwords.txt', threshold=2)


def normalize_name(name, *args, **kwargs):
    processed_name = name
    # if kwargs.get('preprocess', True):
    processed_name = processed_name.translate({ord(c): " " for c in "!@#$%^&*()[]{};:,./<>?\|`~-=_+"})
    processed_name = assignee_preprocessor.preprocess(processed_name)
    # if kwargs.get("lower", True):
    processed_name = processed_name.lower()
    # if kwargs.get("trim_whitespace", True):
    processed_name.strip()
    # if kwargs.get("strip_extra_whitespace", True):
    processed_name = re.sub("(\s){2,}", "\\1", processed_name)
    # if kwargs.get("force_ascii", True):
    processed_name = ''.join([i if ord(i) < 128 else ' ' for i in processed_name])
    # if kwargs.get('normalize', True):
    processed_name = unicodedata.normalize('NFD', processed_name)
    return processed_name


def load_assignee_stopwords():
    r = set()
    with open(f'{project_root}/clustering_resources/assignee-stopwords-lowercase.txt') as fin:
        for line in fin:
            r.add(line.strip())
    r = set()
    return r


assignee_stop_words = load_assignee_stopwords()


def split(name):
    for pat in last_split_patterns:
        splt = re.split(pat, name)
        if len(splt) == 2:
            return splt[1]
    for pat in first_split_patterns:
        splt = re.split(pat, name)
        if len(splt) == 2:
            return splt[0]
    return name


def find_acronyms(name):
    return [x.replace('(', '').replace(')', '') for x in re.findall('(?i)\([A-Z\.\-]*\)', name)]


def remove_stopwords(name):
    toks = word_tokenize(name)
    no_stop_toks = [x for x in toks if x not in assignee_stop_words]
    return " ".join(no_stop_toks)


def remove_punct(name):
    return name.strip(string.punctuation)


punct_trans = str.maketrans(string.punctuation, ' ' * len(string.punctuation))


def replace_punct(name):
    return name.translate(punct_trans)


def clean(name):
    name = re.sub('\([^)]*\)', '', name)
    name = name.replace('"', '').replace('&', ' and ')
    name = replace_punct(name)
    name = re.sub(' [ ]*', ' ', name)
    name = name.strip()
    return name


def relaxed_string_hash(name: str):
    splt = split(name)
    cleaned = clean(splt)
    lc = cleaned.lower()
    noStopwords = remove_stopwords(lc)
    if len(noStopwords) <= 2:
        noStopwords = lc
    noSpaces = noStopwords.replace(' ', '')
    if noSpaces:
        return noSpaces
    elif noStopwords:
        return noStopwords
    elif lc:
        return lc
    elif cleaned:
        return cleaned
    elif name:
        return name
    else:
        logging.warning("Empty Assignee Name!")
        import uuid
        return str(uuid.uuid4())


def assignee_name_features_and_canopies(name: str):
    normalized = normalize_name(name, preprocess=True)
    splt = split(normalized)
    cleaned = clean(splt)
    # lc = cleaned.lower()
    # noStopwords = remove_stopwords(lc)
    # if len(noStopwords) <= 2:
    #     noStopwords = lc
    noSpaces = cleaned.replace(' ', '')
    # canopies = [z for y in noStopwords.split(' ') for z in [y[0:4], y[-4:]] if len(z) >= 3]
    canopies = [y[0:4] for y in cleaned.split(' ') if len(y[0:4]) >= 3]
    if noSpaces:
        rsh = noSpaces
    elif cleaned:
        rsh = cleaned
    elif name:
        rsh = name
    else:
        logging.warning("Empty Assignee Name!")
        rsh = str(uuid.uuid4())
    acronyms = find_acronyms(normalized)
    canopies.extend(acronyms)
    if len(canopies) == 0:
        canopies.append(rsh)
    return [rsh, cleaned] + acronyms, canopies


if __name__ == "__main__":
    print(assignee_name_features_and_canopies('International Business Machines (IBM)'))
    print(normalize_name('International Business Machines (IBM)'))
    print(find_acronyms(normalize_name('International Business Machines (IBM)')))
