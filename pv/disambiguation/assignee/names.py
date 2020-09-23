import re
import string
import unicodedata

from absl import logging
from nltk import word_tokenize

last_split_patterns = ['(?i)as .* by .* of', '(?i)on behalf of', '(?i)as .* by', '(?i)c/o', '(?i)board of regents of']
first_split_patterns = ['(?i)also trading as', '(?i)acting by and through']


def normalize_name(name):
    return re.sub(' [ ]*', ' ', unicodedata.normalize('NFD', name.replace('\"', '')).encode('ascii', 'ignore').decode(
        "utf-8").lower()).strip()


def load_assignee_stopwords():
    r = set()
    with open('resources/assignee-stopwords-lowercase.txt') as fin:
        for line in fin:
            r.add(line.strip())
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
    normalized = normalize_name(name)
    splt = split(normalized)
    cleaned = clean(splt)
    lc = cleaned.lower()
    noStopwords = remove_stopwords(lc)
    if len(noStopwords) <= 2:
        noStopwords = lc
    noSpaces = noStopwords.replace(' ', '')
    canopies = [z for y in noStopwords.split(' ') for z in [y[0:4], y[-4:]] if len(z) >= 3]
    if noSpaces:
        rsh = noSpaces
    elif noStopwords:
        rsh = noStopwords
    elif lc:
        rsh = lc
    elif cleaned:
        rsh = cleaned
    elif name:
        rsh = name
    else:
        logging.warning("Empty Assignee Name!")
        import uuid
        rsh = str(uuid.uuid4())
    acronyms = find_acronyms(normalized)
    canopies.extend(acronyms)
    if len(canopies) == 0:
        canopies.append(rsh)
    return [rsh, noStopwords] + acronyms, canopies


if __name__ == "__main__":
    print(assignee_name_features_and_canopies('International Business Machines (IBM)'))
    print(normalize_name('International Business Machines (IBM)'))
    print(find_acronyms(normalize_name('International Business Machines (IBM)')))
