from os import listdir
from os.path import isfile, join
import pandas as pd


def check_output_blank(filepath):
    files = [f for f in listdir(filepath) if isfile(join(filepath, f))]
    print(files)
    for file in files:
        if 'job' in file:
            print("--------------------------")
            print(f"Checking file {file}")
            print("--------------------------")
            temp = pd.read_pickle(f"{filepath}/{file}")
            for key in temp.keys():
                if temp[key][0]==temp[key][1]:
                    print(f"{key} in {file} IS WRONG: HAS {len(temp[key][0])} & {len(temp[key][1])}")

def find_inventor_id_singleton(filepath, inventor_id_searching):
    files = [f for f in listdir(filepath) if isfile(join(filepath, f))]
    print(files)
    for file in files:
        if 'job' in file:
            print(f"Checking file {file}")
            temp = pd.read_pickle(f"{filepath}/{file}")
            for key in temp.keys():
                try:
                    lookup_attempt = temp[key]
                    test = lookup_attempt[0]
                    for i in test:
                        if i == inventor_id_searching:
                            print("--------------------------")
                            print(f"{file} is the correct file for the ID {inventor_id_searching}!")
                            print("--------------------------")
                            print(
                                f"{inventor_id_searching} returned {len(set(temp[inventor_id_searching][0]))} rows and {len(set(temp[inventor_id_searching][1]))}  unique IDS")
                            print("--------------------------")
                            inventor_freq = freq_distribution_from_list(temp[inventor_id_searching][1])
                            print("Below is each inventor_id and the number of relevant occurrence")
                            print(inventor_freq)
                            print("--------------------------")
                except KeyError:
                    continue


def check_duplicate_uuids(filepath):
    files = [f for f in listdir(filepath) if isfile(join(filepath, f))]
    print(files)
    for file in files:
        if 'singleton' not in file and 'job' in file:
            print(f"Checking file {file}")
            temp = pd.read_pickle(f"{filepath}/{file}")
            for key in temp.keys():
                lookup_attempt = temp[key]
                test = lookup_attempt[0]
                if len(test) != len(set(test)):
                        print("--------------------------")
                        print(f"{file} HAS DUPLICATES FOR  {key}!")
                        print("--------------------------")


def find_top_n_canopies(file, n=10):
    print(f"Checking file {file}")
    temp = pd.read_pickle(f"{file}")
    temp_dict = {}
    for key in temp.keys():
        temp_dict[key] = len(temp[key])
    temp_df = pd.DataFrame(temp_dict.items(), columns=['Key', 'Num'])
    print(temp_df.sort_values(by=['Num'], ascending=False).head(n))


if __name__ == "__main__":
    mypath = '/home/centos/testing_disambig_env/PatentsView-Disambiguation/exp_out/inventor/2022-09-29'
    #check_output_blank(mypath)
    #find_inventor_id_singleton(mypath, 'jw443vh150exrrlohrkvrl3u4')
    check_duplicate_uuids(mypath)