import csv
import time

from sqlalchemy import create_engine
import pandas as pd

# from QA.post_processing.AssigneePostProcessing import AssigneePostProcessingQC
from lib.configuration import get_config, get_connection_string
from pv.disambiguation.assignee.model import EntityKBFeatures

import re
import unicodedata

def update_rawassignee(update_config):
    engine = create_engine(get_connection_string(update_config, "NEW_DB"))
    update_statement = "UPDATE rawassignee ra left join assignee_disambiguation_mapping adm on adm.uuid = ra.uuid set " \
                       "" \
                       "" \
                       "" \
                       "" \
                       "" \
                       "" \
                       "" \
                       "ra.assignee_id=adm.assignee_id "
    engine.execute(update_statement)


def generate_disambiguated_assignees(config,engine, limit, offset):
    assignee_core_template = """
    SELECT assignee_id
    from disambiguated_assignee_ids order by assignee_id
    limit {limit} offset {offset}
    """

    assignee_data_template = """
    SELECT ra.assignee_id, organization, name_first, name_last, p.date as doc_date
    from rawassignee ra
             join patent p on p.id = ra.patent_id
             join ({inv_core_query}) inventor on inventor.assignee_id = ra.assignee_id
    where ra.assignee_id is not null
    UNION 
SELECT app_ra.assignee_id, organization, name_first, name_last, p.date as doc_date
    from   {pgpubs_database}.rawassignee app_ra
             join {pgpubs_database}.publication p on p.document_number =app_ra.document_number
             join ({inv_core_query}) inventor on inventor.assignee_id = app_ra.assignee_id
    where app_ra.assignee_id is not null;
    """
    assignee_core_query = assignee_core_template.format(limit=limit,
                                                        offset=offset)
    assignee_data_query = assignee_data_template.format(pgpubs_database=config['DATABASE_SETUP']['PGPUBS_DATABASE'],
                                                        inv_core_query=assignee_core_query)

    current_assignee_data = pd.read_sql_query(sql=assignee_data_query, con=engine)
    return current_assignee_data

def normalize_name(name):
    return re.sub(' [ ]*', ' ', unicodedata.normalize('NFD', name.replace('\"', '')).encode('ascii', 'ignore').decode(
        "utf-8").lower()).strip()

def assignee_reduce(assignee_data, entity_kb):

    def is_entity_name(x):
        n = x['organization']
        return 1 if normalize_name(n) in entity_kb.emap else 0

    assignee_data['in_entity_kb'] = assignee_data.groupby(['assignee_id', 'organization', 'name_first', 'name_last'])[
        'assignee_id'].transform(is_entity_name,axis=1)

    assignee_data['count'] = assignee_data.groupby(['assignee_id', 'organization', 'name_first', 'name_last'])[
        'assignee_id'].transform('count')
    out = assignee_data.sort_values(['in_entity_kb', 'count', 'doc_date'], ascending=[False, False, False],
                                    na_position='last').drop_duplicates(
            'assignee_id', keep='first').drop(
            ['in_entity_kb', 'count', 'doc_date'], 1)
    return out


def precache_assignee(config):
    assignee_cache_query = """
    INSERT INTO disambiguated_assignee_ids (assignee_id)
    SELECT distinct assignee_id from rawassignee
    UNION
    SELECT distinct assignee_id from {pgpubs_database}.rawassignee;
    """.format(pgpubs_database=config['DATABASE_SETUP']['PGPUBS_DATABASE'])
    engine = create_engine(get_connection_string(config, "NEW_DB"))
    engine.execute(assignee_cache_query)


def create_assignee(update_config):
    entity_kb = EntityKBFeatures('resources/permid_entity_info.pkl', None, None)
    engine = create_engine(get_connection_string(update_config, "NEW_DB"))
    limit = 10000
    offset = 0
    while True:
        start = time.time()
        current_assignee_data = generate_disambiguated_assignees(update_config, engine, limit, offset)
        if current_assignee_data.shape[0] < 1:
            break
        step_time = time.time() - start
        start = time.time()

        # current_assignee_data = current_assignee_data.sort_values(by=['patent_date'])
        # current_assignee_data['inventor_name'] = current_assignee_data['name_first'] + "|" + current_assignee_data[
        #     'name_last']
        # canonical_assignments = current_assignee_data.groupby(['inventor_id'])['inventor_name'].agg(
        #     pd.Series.mode).reset_index()
        # inventor_data = canonical_assignments.join(
        #     canonical_assignments.inventor_name.str.split("|", expand=True).rename({
        #                                                                                    0: 'name_first',
        #                                                                                    1: 'name_last'
        #                                                                                    })).drop("inventor_name",
        #                                                                                             axis=1)
        # canonical_assignments = current_assignee_data.groupby("inventor_id").apply(
        #         assignee_reduce).reset_index(drop=True).rename({
        #         "inventor_id": "id"
        #         }, axis=1)
        step_time = time.time() - start
        canonical_assignments = assignee_reduce(current_assignee_data, entity_kb).rename({
                "assignee_id": "id"
                }, axis=1)
        canonical_assignments.to_sql(name='assignee', con=engine,
                                     if_exists='append',
                                     index=False)
        current_iteration_duration = time.time() - start
        offset = limit + offset


def upload_disambig_results(update_config):
    engine = create_engine(get_connection_string(update_config, "NEW_DB"))
    disambig_output_file = "{wkfolder}/disambig_output/{disamb_file}".format(
            wkfolder=update_config['FOLDERS']['WORKING_FOLDER'], disamb_file="assignee_disambiguation.tsv")
    disambig_output = pd.read_csv(disambig_output_file, sep="\t", chunksize=300000, header=None, quoting=csv.QUOTE_NONE,
                                  names=['unknown_1', 'uuid', 'inventor_id', 'name_first', 'name_middle', 'name_last',
                                         'name_suffix'])
    count = 0
    for disambig_chunk in disambig_output:
        engine.connect()
        start = time.time()
        count += disambig_chunk.shape[0]
        disambig_chunk[["uuid", "inventor_id"
                        ]].to_sql(name='inventor_disambiguation_mapping',
                                  con=engine,
                                  if_exists='append',
                                  index=False,
                                  method='multi')
        end = time.time()
        print("It took {duration} seconds to get to {cnt}".format(duration=round(
                end - start, 3),
                cnt=count))
        engine.dispose()


def post_process_assignee(config):
    # upload_disambig_results(config)
    # update_rawinventor(config)
    precache_assignee(config)
    create_assignee(config)


def post_process_qc(config):
    qc = AssigneePostProcessingQC(config)
    qc.runTests()


if __name__ == '__main__':
    config = get_config()
    # post_process_inventor(config)
    post_process_qc(config)
