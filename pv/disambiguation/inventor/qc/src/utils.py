from itertools import combinations

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from Levenshtein import distance, jaro, jaro_winkler


def get_dataframe_from_pymysql_cursor(connection, query):
    if not connection.open:
        connection.connect()
    with connection.cursor() as cursor:
        cursor.execute(query)
        results = pd.DataFrame(cursor.fetchall(), columns=[i[0] for i in cursor.description])
    return results


def generate_comparitive_violin_plot(old_data, new_data, log=False):
    plt.ioff()
    if log:
        old_y = np.log(old_data)
        new_y = np.log(new_data)
    else:
        old_y = old_data
        new_y = new_data
    fig, axarr = plt.subplots(nrows=1, ncols=2, sharex=True, sharey=True, figsize=(15, 10))
    axarr[0].violinplot(old_y)
    axarr[1].violinplot(new_y)
    return fig,axarr


def calculate_cluster_distances(cluster_data):
    distance_map = {
            "levenshtein":  distance,
            'jaro':         jaro,
            'jaro_winkler': jaro_winkler
            }
    current_cluster_stats = {
            'N':            cluster_data.shape[0],
            'levenshtein':  0,
            'jaro':         0,
            'jaro_winkler': 0
            }
    if cluster_data.shape[0] == 1:
        current_cluster_stats['levenshtein'] = None
        current_cluster_stats['jaro'] = None
        current_cluster_stats['jaro_winkler'] = None
    elif cluster_data.shape[0] > 0:
        for x in combinations(cluster_data.inventor_name, 2):
            for distance_method, distance_function in distance_map.items():
                current_cluster_stats[distance_method] += distance_function(
                        x[0], x[1])

    return pd.Series(current_cluster_stats)
