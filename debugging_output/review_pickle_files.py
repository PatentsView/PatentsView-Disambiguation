from os import listdir
from os.path import isfile, join
import pandas as pd
import datetime
from lib.configuration import get_current_config, get_unique_connection_string
from sqlalchemy import create_engine


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

# def find_inventor_id_singleton(filepath, inventor_id_searching):
#     files = [f for f in listdir(filepath) if isfile(join(filepath, f))]
#     print(files)
#     for file in files:
#         if 'job' in file:
#             print(f"Checking file {file}")
#             temp = pd.read_pickle(f"{filepath}/{file}")
#             for key in temp.keys():
#                 try:
#                     lookup_attempt = temp[key]
#                     test = lookup_attempt[0]
#                     for i in test:
#                         if i == inventor_id_searching:
#                             print("--------------------------")
#                             print(f"{file} is the correct file for the ID {inventor_id_searching}!")
#                             print("--------------------------")
#                             print(
#                                 f"{inventor_id_searching} returned {len(set(temp[inventor_id_searching][0]))} rows and {len(set(temp[inventor_id_searching][1]))}  unique IDS")
#                             print("--------------------------")
#                             inventor_freq = freq_distribution_from_list(temp[inventor_id_searching][1])
#                             print("Below is each inventor_id and the number of relevant occurrence")
#                             print(inventor_freq)
#                             print("--------------------------")
#                 except KeyError:
#                     continue


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

def convert_pickle_to_table(file):
    data = pd.read_pickle(file)
    temp_list = []
    for key in data.keys():
        temp = [data[key].uuid, data[key].name_hash, str(data[key].name_features), str(data[key].mention_ids), str(data[key].record_ids), str(data[key].location_strings), str(data[key].canopies), str(data[key].unique_exact_strings), data[key].normalized_most_frequent]
        temp_list.append(temp)
    final = pd.DataFrame(temp_list, columns=['uuid', 'name_hash', 'name_features', 'mention_ids', 'record_ids', 'location_strings', 'canopies', 'unique_exact_strings', 'normalized_most_frequent'])
    qa_engine = get_engine()
    final.to_sql("assignee_mentions_20230330", if_exists='append', con=qa_engine, index=False)

# def convert_final_jsons(final):
#     columns_to_convert = ["name_features", "mention_ids", "record_ids","canopies", "location_strings","unique_exact_strings"]
#     for col in columns_to_convert:
#         final[col] = final[col].apply(json.dumps)
#     return final

def get_engine():
    config = get_current_config(**{'execution_date': datetime.date(2023, 1, 1)})
    qa_connection_string = get_unique_connection_string(config, database='patent_disambiguation_testing', connection='DATABASE_SETUP')
    qa_engine = create_engine(qa_connection_string)
    return qa_engine


# d = {"key": }

if __name__ == "__main__":
    # mypath = '/home/centos/testing_disambig_env/PatentsView-Disambiguation/exp_out/inventor/2022-09-29'
    #check_output_blank(mypath)
    #find_inventor_id_singleton(mypath, 'jw443vh150exrrlohrkvrl3u4')
    # check_duplicate_uuids(mypath)
    breakpoint()
    convert_pickle_to_table("assignee_mentions.records.pkl")