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

    old_stats = pd.DataFrame(old_disambig_data.describe())

    new_stats = pd.DataFrame(new_disambig_data.describe())

    old_stats.join(new_stats, lsuffix="_old", rsuffix="_new")
    fig,plots = generate_comparitive_violin_plot(old_disambig_data.cluster_size, new_disambig_data.cluster_size, log=True)

    plots[0].set_ylabel("Log of Cluster Sizes")
    plots[0].set_title("Cluster Size (Old Disambiguation)")
    plots[0].set_xticks([])
    plots[1].set_ylabel("Log of Cluster Sizes")
    plots[1].set_title("Cluster Size (New Disambiguation)")
    plots[0].set_xticks([])
    fig.savefig('qc_assignee_size_violin.pdf')

if __name__ == "__main__":
    main()