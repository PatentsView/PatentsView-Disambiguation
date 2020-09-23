import pickle


class Loader(object):
    def __init__(self, assignee_canopies, assignee_mentions):
        self.assignee_canopies = assignee_canopies
        self.assignee_mentions = assignee_mentions

    def load(self, canopy):
        return [self.assignee_mentions[mid] for mid in self.assignee_canopies[canopy]]

    def num_records(self, canopy):
        return len(self.assignee_canopies[canopy])

    @staticmethod
    def from_flags(flgs):
        with open(flgs.assignee_canopies, 'rb') as fin:
            canopies = pickle.load(fin)
        with open(flgs.assignee_mentions, 'rb') as fin:
            assignee_mentions = pickle.load(fin)
        l = Loader(canopies, assignee_mentions)
        return l
