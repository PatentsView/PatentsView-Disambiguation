import pickle


class Loader(object):
    def __init__(self, assignee_canopies, assignee_mentions):
        self.assignee_canopies = assignee_canopies
        self.assignee_mentions = assignee_mentions

    def load(self, canopy: object) -> object:
        return [self.assignee_mentions[mid] for mid in self.assignee_canopies[canopy]]

    def num_records(self, canopy):
        return len(self.assignee_canopies[canopy])

    @staticmethod
    def from_config(config):
        end_date = config["DATES"]["END_DATE_DASH"]
        path = f"{config['BASE_PATH']['assignee']}".format(end_date=end_date) + config['BUILD_ASSIGNEE_NAME_MENTIONS'][
            'feature_out']
        with open(path + '.%s.pkl' % 'canopies', 'rb') as fin:
            canopies = pickle.load(fin)
        with open(path + '.%s.pkl' % 'records', 'rb') as fin:
            assignee_mentions = pickle.load(fin)
        l = Loader(canopies, assignee_mentions)
        return l
