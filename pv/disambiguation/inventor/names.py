import unicodedata


def normalize_name(name):
    return unicodedata.normalize('NFD', name.replace('\"', '')).encode('ascii', 'ignore').decode("utf-8").lower()


def is_initial(name):
    if len(name) == 1:
        return True
    elif len(name) == 2 and name[1] == '.':
        return True
    elif len(name) == 3 and name[1] == '-':
        return True
    else:
        return False


def first_char_or_empty(name):
    return [name[0]] if len(name) > 0 else []


def full_name_or_empty(name):
    return [name] if len(name) > 0 else []


def first_initial(name):
    name = normalize_name(name)
    if '-' in name:
        splt = name.split('-')
        c1 = first_char_or_empty(splt[0])
        c2 = first_char_or_empty(splt[1])
        if c1 and c2:
            return ['%s-%s' % (c1[0] if c1 else '', c2[0] if c2 else '')]
        else:
            return []
    else:
        return first_char_or_empty(name)


def first_letter(name):
    name = normalize_name(name)
    return first_char_or_empty(name)


def first_name(name):
    name = normalize_name(name)
    name = name.replace('-', ' ')
    if name:
        splt = name.split(' ')
        name = splt[0]
    if not is_initial(name):
        return full_name_or_empty(name)
    else:
        return []


def middle_name(name):
    name = normalize_name(name)
    name = name.replace('-', ' ')
    if ' ' in name:
        splt = name.split(' ')
        name = splt[1]
    else:
        return []
    if name and not is_initial(name):
        return full_name_or_empty(name)
    else:
        return []


def middle_initial(name):
    name = normalize_name(name)
    name = name.replace('-', ' ')
    if ' ' in name:
        splt = name.split(' ')
        name = splt[1]
    else:
        return []
    if name:
        return first_char_or_empty(name)
    else:
        return []


suffix_list = {'jr', 'j.r.', 'jr.', 'junior', 'sr', 's.r.', 'sr.', 'senior', 'iii', 'i.i.i.', 'iii.', 'iv', 'i.v.',
               'iv.', 'v', 'v.', 'v.i.', 'v.i.i.', 'v.i.i.i', 'ix', 'i.x.', 'ix.', 'x', 'x.'}


def suffixes(name):
    name = normalize_name(name)
    tokens1 = set([x.strip() for x in name.split(' ')])
    tokens2 = set([x.strip() for x in name.split(',')])
    matching = list(suffix_list.intersection(tokens1).union(suffix_list.intersection(tokens2)))
    return matching


def last_name(name):
    name = normalize_name(name)
    if ',' in name:
        splt = name.split(',')
        name = splt[0]
    if name:
        return full_name_or_empty(name)
    else:
        return []
