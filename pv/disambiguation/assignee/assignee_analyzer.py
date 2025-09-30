import re
import json
import unicodedata
import editdistance



# %%
def load_assignee_analyzer_configuration(assignee_abbreviation_file, assignee_correction_file, assignee_stopphrase_file, assignee_bh_file):
    correction_configuration = list()
    with open(assignee_correction_file) as fin:
        for line in fin:
            correction_configuration.append(line.strip())
    stopphrase_configuration = list()
    with open(assignee_stopphrase_file) as fin:
        for line in fin:
            stopphrase_configuration.append(line.strip())
    remapping_configuration = json.load(open(assignee_abbreviation_file, 'r'))
    bh_configuration = json.load(open(assignee_bh_file, 'r'))
    return correction_configuration, stopphrase_configuration, remapping_configuration, bh_configuration


# %%
def char_wb_ngram_with_lower_priority_exclusion(text_document, ngram_range, white_spaces,
                                                exclusion_list=None):
    """Whitespace sensitive char-n-gram tokenization.
    Tokenize text_document into a sequence of character n-grams
    operating only inside word boundaries. n-grams at the edges
    of words are padded with space."""
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


def analyze_assignee_name(assignee_name, *args, **kwargs):
    N_GRAM_RANGE = (2, 10)
    path = "/project/clustering_resources/"
    assignee_abbreviation_file = path + "assignee_abbreviations.json"
    assignee_correction_file = path + "assignee_corrections.txt"
    assignee_stopphrase_file = path + "assignee_stopwords.txt"
    assignee_bronwyn_hall_file = path + "assignee_bronwyn_hall.json"

    CORRECTION_CONFIGURATION, STOPPHRASE_CONFIGURATION, REMAPPING_CONFIGURATION, BH_CONFIGURATION = load_assignee_analyzer_configuration(
        assignee_abbreviation_file, assignee_correction_file, assignee_stopphrase_file, assignee_bronwyn_hall_file)
    def remove_stopphrase(doc):
        for stopphrase in STOPPHRASE_CONFIGURATION:
            doc = re.sub(r"\b" + stopphrase + r"\b", " ", doc).strip()
        return doc

    def correct_words_bronwyn_hall(doc):
        for rawassignee_words, bronwyn_hall_replacement in BH_CONFIGURATION.items():
            doc = re.sub(rawassignee_words, bronwyn_hall_replacement, doc)
        return doc

    def correct_tokens(token):
        THRESHOLD = 2
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

    assignee_name = assignee_name.translate({ord(c): " " for c in "!@#$%^&*()[]{};:,./<>?\|`~-=_+\"'"})
    assignee_name = unicodedata.normalize('NFD', assignee_name)
    assignee_name = expand_abbreviation(assignee_name)
    assignee_name = remove_stopphrase(assignee_name)
    assignee_name = correct_words_bronwyn_hall(assignee_name)
    return char_wb_ngram_with_lower_priority_exclusion(assignee_name, exclusion_list=CORRECTION_CONFIGURATION,
                                                       ngram_range=kwargs.get('ngram_range', N_GRAM_RANGE),
                                                       white_spaces=kwargs.get('white_spaces', re.compile(r"\s\s+")))