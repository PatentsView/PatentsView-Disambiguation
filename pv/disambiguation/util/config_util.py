import datetime
import os


def generate_incremental_components(config, source, db_table_prefix):
    where_clause = ""
    if config['DISAMBIGUATION']['INCREMENTAL'] == "1":
        where_clause = "where {prefix}.version_indicator between '{start_date}' and '{end_date}'".format(
            prefix=db_table_prefix,
            start_date=config['DATES']['START_DATE'], end_date=config['DATES']['END_DATE'])
        # | id | document_number | sequence | name_first | name_last | organization | type |
        # rawlocation_id | city | state | country | filename | created_date | updated_date |
    id_field = "id"
    document_id_field = "document_number"
    sequence_field = "sequence - 1"
    title_table = "application"
    title_field = "invention_title"
    record_id_format = "pg-%s"
    if source == "granted_patent_database":
        id_field = "uuid"
        document_id_field = "id"
        sequence_field = "sequence"
        title_field = "title"
        title_table = "patent"
        record_id_format = "%s"

    return {"id_field": id_field, "document_id_field": document_id_field,
            "sequence_field": sequence_field, "title_field": title_field,
            "title_table": title_table, "filter": where_clause,
            "record_id_format": record_id_format}


def prepare_inventor_config(config):
    config['inventor']['run_id'] = config['DATES']['END_DATE']
    canopy_path = config['INVENTOR_BUILD_CANOPIES']['canopy_out']
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
    suffix = datetime.datetime.strptime(config['DATES']['END_DATE'], "%Y-%m-%d").strftime("%Y-%m-%d")
    config['INVENTOR_UPLOAD']['target_table'] = "inventor_disambiguation_mapping_{}".format(suffix)
    return config
