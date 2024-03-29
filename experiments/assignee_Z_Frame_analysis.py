import pandas as pd
import seaborn as sns
import glob
import os
from thefuzz import fuzz
from tqdm.auto import tqdm
# from tdqm import tqdm
import numpy as np
from thefuzz import fuzz

def get_subpopulation(frame, term):
    return frame[frame.set_1_labels.str.contains(term) | (frame.set_2_labels.str.startswith(term))]


def find_max_distance(record):
    max_distance = 0
    set1 = eval(record.set_1_labels)
    set2 = eval(record.set_2_labels)
    if len(set1) > 50 or len(set2) > 50:
        return np.Inf
    for one_item in set1:
        for two_item in set2:
            distance = 100 - fuzz.partial_token_set_ratio(one_item, two_item)
            if distance > max_distance:
                max_distance = distance
    return max_distance

# def alternative(df):
#     for index, row in tqdm(df.iterrows(), total=df.shape[0]):
#         find_max_distance(row)

def plot_Z_v_text_distance(Z):
    tqdm.pandas()
    Z = Z.assign(text_distance=Z.progress_apply(find_max_distance, axis=1))
    # Z = Z.assign(text_distance=Z.progress_apply(lambda x: find_max_distance(x), axis=1))
    Z = Z.assign(text_cut=pd.cut(Z.text_distance, range(0, 100, 5)))
    Z = Z.assign(cd_cut=pd.cut(Z.distance, [x / 100 for x in range(0, 100, 5)]))
    cut = pd.cut(Z.distance, [x / 100 for x in range(0, 100, 2)])
    boxdf = Z.groupby(cut).apply(lambda df: df.text_distance.reset_index(drop=True)).unstack(0)
    g = sns.boxplot(data=boxdf, orient='v', )
    g.figure.set_size_inches(50, 20)
    g.figure.savefig("assignee_z_performance.png")

def run_analysis(path):
    frame_files = []
    for num in range(0,4):
        temp = path + f'Z_Frame_job-{num}.csv'
        frame_files.append(temp)
    print(frame_files)
    Z_frames = []
    for frame_file in frame_files:
        try:
            Z_frames.append(pd.read_csv(frame_file, index_col=0))
        except FileNotFoundError:
            continue
    print(Z_frames)
    Z_frame = pd.concat(Z_frames)
    Z_frame = Z_frame.assign(cd_cut=pd.cut(Z_frame.distance, [x / 100 for x in range(0, 100, 5)]))
    print("Starting Plot Functions ...")
    print(Z_frame.head())
    plot_Z_v_text_distance(Z_frame)

if __name__ == '__main__':
    path = "/Users/bcard/projects/patentsview/PatentsView-DB/"
    run_analysis(path)