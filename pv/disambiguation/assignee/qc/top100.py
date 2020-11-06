import os
import matplotlib.pyplot as plt
import pandas as pd
import mysql.connector

import pymysql
from pv.disambiguation.util.qc_utils import get_dataframe_from_pymysql_cursor, generate_comparitive_violin_plot

def main():
    granted_db = pymysql.connect(read_default_file="~/.mylogin.cnf", database='patent_20200630')

    old_disambig_data_query = """
    select count(1) as cluster_size, assignee_id
    from rawassignee
    group by assignee_id;
    """

    old_disambig_data = get_dataframe_from_pymysql_cursor(granted_db, old_disambig_data_query)

    new_disambig_data_query = """
    SELECT disambiguated_id, count(1) as cluster_size
    from tmp_assignee_disambiguation_granted
    group by disambiguated_id;
    """

    new_disambig_data = get_dataframe_from_pymysql_cursor(granted_db, new_disambig_data_query)

    top100old = old_disambig_data.nlargest(100, 'cluster_size')
    top100new = new_disambig_data.nlargest(100, 'cluster_size')

    get_entity_name_new = """
        SELECT DISTINCT(organization) from rawassignee INNER JOIN tmp_assignee_disambiguation_granted ON rawassignee.uuid=tmp_assignee_disambiguation_granted.uuid WHERE disambiguated_id="%s" LIMIT 10;
        """

    get_entity_name_old = """
            SELECT DISTINCT(organization) from rawassignee WHERE assignee_id="%s" LIMIT 10;
            """
    from tqdm import tqdm
    with open('assignee_top_100_old.txt', 'w') as fout:
        for idx, val in tqdm(top100old.iterrows()):
            this_q = (get_entity_name_old + '') % val[1]
            with granted_db.cursor() as cursor:
                cursor.execute(this_q)
                results = pd.DataFrame(cursor.fetchall(), columns=[i[0] for i in cursor.description])
                names = [x for x in results['organization']]
                name_str = '; '.join(names)
                fout.write('%s\t%s\t%s\n' % (val[0], val[1], name_str))
                cursor.close()

    with open('assignee_top_100_new.txt', 'w') as fout:
        for idx, val in tqdm(top100new.iterrows()):
            this_q = (get_entity_name_new + '') % val[0]
            with granted_db.cursor() as cursor:
                cursor.execute(this_q)
                results = pd.DataFrame(cursor.fetchall(), columns=[i[0] for i in cursor.description])
                names = [x[1][0] for x in results.iterrows()]
                name_str = '; '.join([str(x) for x in names])
                fout.write('%s\t%s\t%s\n' % (val[0], val[1], name_str))
                cursor.close()


if __name__ == "__main__":
    main()