import pickle

class Loader(object):
    def __init__(self, name_mentions):
        self.name_mentions = name_mentions

    def load(self, canopy):
        return self.name_mentions[canopy]

    def num_records(self, canopy):
        return len(self.name_mentions[canopy])

    @staticmethod
    def from_flags(flgs):
        with open(flgs.inventor_location_name_mentions, 'rb') as fin:
            inventor_name_mentions = pickle.load(fin)
        with open(flgs.assignee_location_name_mentions, 'rb') as fin:
            assignee_name_mentions = pickle.load(fin)
        name_mentions = inventor_name_mentions
        name_mentions.update(assignee_name_mentions)
        l = Loader(name_mentions)
        return l
