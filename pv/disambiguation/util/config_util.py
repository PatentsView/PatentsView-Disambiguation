import datetime
import os


def generate_incremental_components(config, source, db_table_prefix, ignore_filters):
    where_clause = "where 1=1"
    if not ignore_filters:
        if 'INCREMENTAL' in config['DISAMBIGUATION'] and config['DISAMBIGUATION']['INCREMENTAL'] == "1":
            where_clause = "where {prefix}.version_indicator between '{start_date}' and '{end_date}'".format(
                prefix=db_table_prefix,
                start_date=config['DATES']['START_DATE'], end_date=config['DATES']['END_DATE'])
        # | id | document_number | sequence | name_first | name_last | organization | type |
        # rawlocation_id | city | state | country | filename | created_date | updated_date |
        else:
            where_clause = "where {prefix}.version_indicator <= '{end_date}'".format(
                prefix=db_table_prefix, end_date=config['DATES']['END_DATE'])
    id_field = "id"
    document_id_field = "document_number"
    central_entity_field = "document_number"
    sequence_field = "sequence - 1"
    title_table = "application"
    title_field = "invention_title"
    record_id_format = "pg-%s"
    if source == "granted_patent_database":
        id_field = "uuid"
        document_id_field = "patent_id"
        central_entity_field = "id"
        sequence_field = "sequence"
        title_field = "title"
        title_table = "patent"
        record_id_format = "%s"

    return {"id_field": id_field.format(table_prefix=db_table_prefix),
            "central_entity_field": central_entity_field.format(table_prefix=db_table_prefix),
            "document_id_field": document_id_field.format(table_prefix=db_table_prefix),
            "sequence_field": sequence_field.format(table_prefix=db_table_prefix),
            "title_field": title_field.format(table_prefix=db_table_prefix),
            "title_table": title_table.format(table_prefix=db_table_prefix),
            "filter": where_clause.format(table_prefix=db_table_prefix),
            "record_id_format": record_id_format}


def prepare_config(config):
    config['inventor']['run_id'] = config['DATES']['END_DATE']
    canopy_path = config['INVENTOR_BUILD_CANOPIES']['canopy_out']

    date_formatted = datetime.datetime.strptime(config['DATES']['END_DATE'], "%Y%m%d").strftime("%Y-%m-%d")
    canopy_path = f"/project/data/{date_formatted}/inventor"
    config['inventor']['pregranted_canopies'] = "{out_path}.{source}.pkl".format(out_path=canopy_path,
                                                                                 source='pregranted')
    config['inventor']['granted_canopies'] = "{out_path}.{source}.pkl".format(out_path=canopy_path,
                                                                              source='granted')
    config['inventor']['patent_titles'] = "{out_path}.{source}.pkl".format(
        out_path=config['INVENTOR_BUILD_TITLES']['feature_out'],
        source='both')
    config['inventor']['coinventors'] = "{out_path}.{source}.pkl".format(
        out_path=config['INVENTOR_BUILD_COINVENTOR_FEAT']['feature_out'],
        source='both')
    config['inventor']['assignees'] = "{out_path}.{source}.pkl".format(
        out_path=config['INVENTOR_BUILD_ASSIGNEE_FEAT']['feature_out'],
        source='both')
    config['inventor']['clustering_output_folder'] = os.path.join(config['inventor']['outprefix'], 'inventor',
                                                                  config['inventor']['run_id'])
    suffix = datetime.datetime.strptime(config['DATES']['END_DATE'], "%Y-%m-%d").strftime("%Y%m%d")
    config['INVENTOR_UPLOAD'] = {}
    config['INVENTOR_UPLOAD']['target_table'] = "inventor_disambiguation_mapping_{}".format(suffix)

    ### Assignee
    config['assignee']['run_id'] = config['DATES']['END_DATE']
    path = config['BUILD_ASSIGNEE_NAME_MENTIONS']['feature_out']
    config['assignee']['assignee_canopies'] = "{out_path}.canopies.pkl".format(out_path=path)
    config['assignee']['assignee_mentions'] = "{out_path}.records.pkl".format(out_path=path)
    config['assignee']['patent_titles'] = config['inventor']['patent_titles']
    config['assignee']['coinventors'] = config['inventor']['patent_titles']
    config['assignee']['assignees'] = config['inventor']['patent_titles']

    config['assignee']['clustering_output_folder'] = os.path.join(config['assignee']['outprefix'], 'assignee',
                                                                  config['assignee']['run_id'])
    config['ASSIGNEE_UPLOAD']['target_table'] = "assignee_disambiguation_mapping_{}".format(suffix)

    return config
