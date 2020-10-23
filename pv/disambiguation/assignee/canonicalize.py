

import os
import matplotlib.pyplot as plt
import pandas as pd
import mysql.connector
import collections
from tqdm import tqdm

import pymysql
from pv.disambiguation.util.qc_utils import get_dataframe_from_pymysql_cursor, generate_comparitive_violin_plot
from pv.disambiguation.assignee.model import EntityKBFeatures
from pv.disambiguation.assignee.names import normalize_name
def main():
    granted_db = pymysql.connect(read_default_file="~/.mylogin.cnf", database='patent_20200630')

    old_disambig_data_query = """
    select count(1) as cluster_size, assignee_id
    from rawassignee
    group by assignee_id;
    """

    old_disambig_data = get_dataframe_from_pymysql_cursor(granted_db, old_disambig_data_query)

    mention_query = """
    SELECT rawassignee.uuid, disambiguated_id, organization, name_first, name_last 
    from rawassignee INNER JOIN tmp_assignee_disambiguation_granted ON rawassignee.uuid=tmp_assignee_disambiguation_granted.uuid;
    """

    mention_data = get_dataframe_from_pymysql_cursor(granted_db, mention_query).to_numpy()

    entity_id_query = """
        SELECT DISTINCT(disambiguated_id)
        from tmp_assignee_disambiguation_granted;
    """

    entity_data = get_dataframe_from_pymysql_cursor(granted_db, entity_id_query).to_numpy()

    entity2namecount = collections.defaultdict(dict)
    for i in tqdm(range(mention_data.shape[0]), 'counting', mention_data.shape[0]):
        name = mention_data[i][2] if mention_data[i][2] else '%s %s' % (mention_data[i][3], mention_data[i][4])
        if name not in entity2namecount[mention_data[i][1]]:
            entity2namecount[mention_data[i][1]][name] = 1
        else:
            entity2namecount[mention_data[i][1]][name] += 1


    # Algorithm:
    # If linked, use name of PermID entity
    # Otherwise pick most frequently appearing name in the cluster (i.e. largest number of patents determines frequency)
    # Normalize characters not displayed in html
    # TODO: Normalize & Tie-break!

    entity_kb = EntityKBFeatures('data/assignee/permid/permid_entity_info.pkl', None, None)
    canonical_names = dict()
    for entity,name2count in tqdm(entity2namecount.items(), 'canonicalizing'):
        sorted_pairs = sorted([(n,c) for n,c in name2count.items()], key=lambda x:x[1], reverse=True)
        for n,c in sorted_pairs:
            if normalize_name(n) in entity_kb.emap:
                canonical_names[entity] = n
                break
        if entity in canonical_names:
            continue
        else:
            canonical_names[entity] = sorted_pairs[0][0]

    with open('assignee_canonical.pkl', 'wb') as fout:
        import pickle
        pickle.dump(canonical_names,fout)


if __name__ == "__main__":
    main()