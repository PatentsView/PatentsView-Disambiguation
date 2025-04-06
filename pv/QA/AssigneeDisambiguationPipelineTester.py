import os
import pickle
from datetime import datetime, timezone
from pv.QA.testing_utils import get_file_modified_time


class AssigneeDisambiguationPipelineTester():
    def __init__(self, config):
        self.config = config
        super().__init__()

    def test_assignee_mentions_step(self, airflow_run_date):
        # Local imports
        from pv.disambiguation.core import AssigneeNameMention
        # Config setup
        end_date = self.config["DATES"]["END_DATE_DASH"]
        path = f"{self.config['BASE_PATH']['assignee']}".format(end_date=end_date) + \
               self.config['BUILD_ASSIGNEE_NAME_MENTIONS']['feature_out']
        print(path)
        # Variables to be used in testing
        name_mention_attributes = ['uuid', 'name_hash', 'name_features', 'mention_ids', 'record_ids',
                                   'location_strings', 'canopies', 'unique_exact_strings',
                                   'normalized_most_frequent']
        pickle_file_path = path + '.%s.pkl' % 'records'
        canopy_file_path = path + '.%s.pkl' % 'canopies'

        records_last_modified_date = get_file_modified_time(pickle_file_path)
        canopies_last_modified_date = get_file_modified_time(canopy_file_path)
        # loading
        airflow_run_date = airflow_run_date.replace(tzinfo=None)
        records = pickle.load(open(pickle_file_path, 'rb'))
        print("=============================================")
        print(records_last_modified_date)
        print(canopies_last_modified_date)
        print(airflow_run_date)
        print("=============================================")

        ## Assertions go here
        # Make sure files exist. (This can go away if we add test for canopies)
        assert os.path.isfile(pickle_file_path)
        assert os.path.isfile(canopy_file_path)
        #
        assert records_last_modified_date > airflow_run_date
        assert canopies_last_modified_date > airflow_run_date

        # Test empty file
        assert len(records) > 0
        # Test the data is dict
        assert isinstance(records, dict)

        for mention_id, mention_object in records.items():
            # Test members are expected object
            assert isinstance(mention_object, AssigneeNameMention)
            # Test each object has the information we calculated in step 1
            assert list(vars(mention_object)) == name_mention_attributes and sorted(
                list(vars(mention_object))) == sorted(name_mention_attributes)
            # Test location string are not actual locations
            # No space in location IDs
            assert all([len(location_string.split()) < 2 for location_string in mention_object.location_strings])
