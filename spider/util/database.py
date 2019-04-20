import json
import tempfile
from typing import Iterable, List

import pandas
from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from more_itertools import first_true, grouper, always_iterable

from spider.constant import BIGQUERY_LOCATION
from spider.util import log_info


def get_table_reference(
    bigquery_client: bigquery.Client,
        dataset_id: str,
        table_id: str) -> bigquery.TableReference:
    dataset_ref = bigquery_client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    return table_ref


def merge_schema(
        old_schema: List[bigquery.schema.SchemaField],
        new_schema: List[bigquery.schema.SchemaField]) -> List[bigquery.schema.SchemaField]:

    if old_schema is None:
        return new_schema

    merged_schema = []
    for new_field in new_schema:
        old_field = first_true(old_schema, default=None,
                               pred=lambda old_field: old_field.name == new_field.name)
        updated_field = bigquery.schema.SchemaField(
            name=new_field.name,
            field_type=new_field.field_type,
            description=old_field.description if old_field is not None else None,
            mode=new_field.mode,
            fields=new_field.fields
        )
        merged_schema.append(updated_field)

    return merged_schema


def get_table_schema(
    bigquery_client: bigquery.Client,
        dataset_id: str,
        table_id: str) -> List[bigquery.schema.SchemaField]:
    table_ref = get_table_reference(bigquery_client, dataset_id, table_id)

    try:
        table = bigquery_client.get_table(table_ref)
    except NotFound as e:
        table = None
        log_info("Table not found." + str(e))

    return table.schema if table else None


def update_table_schema(
    bigquery_client: bigquery.Client,
        dataset_id: str,
        table_id: str,
        schema: List[bigquery.schema.SchemaField]) -> bigquery.Table:
    table_ref = get_table_reference(bigquery_client, dataset_id, table_id)
    table = bigquery.Table(table_ref, schema=schema)
    table = bigquery_client.update_table(table, ['schema'])
    log_info("Updated Schema: {schema}".format(
        schema=table.schema
    ))
    return table


def persist_table_schema(
    bigquery_client: bigquery.Client,
        schema_dataset_id: str,
        schema_table_id: str,
        dataset_id: str,
        table_id: str) -> List[bigquery.schema.SchemaField]:
    old_schema = get_table_schema(bigquery_client, schema_dataset_id, schema_table_id)
    new_schema = get_table_schema(bigquery_client, dataset_id, table_id)
    persisted_schema = merge_schema(old_schema, new_schema)

    updated_table = update_table_schema(
        bigquery_client, dataset_id, table_id, persisted_schema)
    return updated_table.schema


def get_table_id_list(
    bigquery_client: bigquery.Client,
        dataset_id: str) -> list:
    dataset_ref = bigquery_client.dataset(dataset_id)
    table_list = list(bigquery_client.list_tables(dataset_ref))
    table_id_list = [table.table_id for table in table_list]
    return table_id_list


def get_dataset_id_list(bigquery_client: bigquery.Client) -> list:
    return [dataset.dataset_id for dataset in
            list(bigquery_client.list_datasets())]


def copy_to_table(source_dataset_id: str,
                  source_table_id: str,
                  destination_dataset_id: str,
                  destination_table_id: str,
                  bigquery_conn_id: str) -> str:
    hook = BigQueryHook(bigquery_conn_id)
    conn = hook.get_conn()
    cursor = conn.cursor()
    job_id = cursor.run_copy(
        source_project_dataset_tables='{dataset_id}.{table_id}'.format(
            dataset_id=source_dataset_id, table_id=source_table_id
        ),
        destination_project_dataset_table='{dataset_id}.{table_id}'.format(
            dataset_id=destination_dataset_id, table_id=destination_table_id
        )
    )
    return job_id


def upload_dict_list_to_bigquery(
    bigquery_client: bigquery.Client,
        dict_list: List[dict],
        destination_dataset_id: str,
        destination_table_id: str,
        schema: List[bigquery.schema.SchemaField],
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE):
    with tempfile.NamedTemporaryFile('w', suffix='.json') as file:
        for dict in dict_list:
            file.write(json.dumps(dict) + '\n')
        file.seek(0)
        upload_json_to_bigquery(
            bigquery_client=bigquery_client,
            destination_dataset_id=destination_dataset_id,
            destination_table_id=destination_table_id,
            source_filepath=file.name,
            schema=schema,
            write_disposition=write_disposition
        )


def upload_json_to_bigquery(
    bigquery_client: bigquery.Client,
        destination_dataset_id: str,
        destination_table_id: str,
        source_filepath: str,
        schema: List[bigquery.schema.SchemaField],
        location: str = 'US',
        write_disposition: bigquery.WriteDisposition = bigquery.WriteDisposition.WRITE_TRUNCATE,
        max_bad_records: int = 0,
) -> str:
    table_ref = get_table_reference(
        bigquery_client, destination_dataset_id, destination_table_id)
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job_config.schema = schema
    job_config.write_disposition = write_disposition
    with open(source_filepath, 'rb') as source_file:
        job = bigquery_client.load_table_from_file(
            source_file,
            table_ref,
            location=BIGQUERY_LOCATION,
            job_config=job_config,
        )

    job.result()
    log_info(
        f'Loaded {job.output_rows} rows into {destination_dataset_id}:{destination_table_id}.')
    return job.job_id


def upload_csv_to_bigquery(
    bigquery_client: bigquery.Client,
        destination_dataset_id: str,
        destination_table_id: str,
        source_filepath: str,
        schema: List[bigquery.schema.SchemaField],
        location: str = 'US',
        leading_rows: int = 1,
        write_disposition: bigquery.WriteDisposition = bigquery.WriteDisposition.WRITE_TRUNCATE,
        max_bad_records: int = 0,
) -> str:
    table_ref = get_table_reference(
        bigquery_client, destination_dataset_id, destination_table_id)
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.skip_leading_rows = leading_rows
    job_config.schema = schema
    job_config.write_disposition = write_disposition
    with open(source_filepath, 'rb') as source_file:
        job = bigquery_client.load_table_from_file(
            source_file,
            table_ref,
            location=BIGQUERY_LOCATION,
            job_config=job_config,
        )

    job.result()
    log_info(
        f'Loaded {job.output_rows} rows into {destination_dataset_id}:{destination_table_id}.')
    return job.job_id


def upload_dict_iterable_to_bigquery(
    bigquery_client: bigquery.Client,
        dict_iterable: Iterable[dict],
        destination_dataset_id: str,
        destination_table_id: str,
        schema: List[bigquery.schema.SchemaField],
        chunk_size: int = 10000) -> None:
    """
    The first chunk uploaded will created a new table and the
    remaining chunks will be appended to the existing table.
    This makes sure that each chunk is of a managable size,
    in case the data is too large preventing issues caused by
    network disruptions or the request being too large.
    """
    chunks = grouper(chunk_size, dict_iterable)
    first_chunk = first_true(chunks)
    upload_dict_list_to_bigquery(
        bigquery_client=bigquery_client,
        dict_list=list(filter(bool, first_chunk)),
        destination_dataset_id=destination_dataset_id,
        destination_table_id=destination_table_id,
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)

    for chunk in chunks:
        upload_dict_list_to_bigquery(
            bigquery_client=bigquery_client,
            dict_list=list(filter(bool, chunk)),
            destination_dataset_id=destination_dataset_id,
            destination_table_id=destination_table_id,
            schema=schema,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND)


def load_query_to_bigquery_table(
    bigquery_client: bigquery.Client,
        sql: str,
        destination_dataset_id: str,
        destination_table_id: str,
        location: str = 'US',
        write_disposition: str = bigquery.WriteDisposition.WRITE_TRUNCATE,
        create_disposition: str = bigquery.CreateDisposition.CREATE_IF_NEEDED) -> str:

    table_ref = get_table_reference(bigquery_client, destination_dataset_id, destination_table_id)
    job_config = bigquery.QueryJobConfig()
    job_config.destination = table_ref
    job_config.write_disposition = write_disposition
    job_config.create_disposition = create_disposition

    query_job = bigquery_client.query(
        sql,
        location=location,
        job_config=job_config
    )

    query_job.result()
    log_info(f'Query results loaded to table {table_ref.path} with job {query_job.job_id}')
    return query_job.job_id


# only use this for small amounts of data to prevent memory issues
# Use the bigquery_to_gcs operator instead for larger amounts of data
def get_dataframe_from_bigquery_table(
    bigquery_client: bigquery.Client,
        dataset_id: str,
        table_id: str) -> pandas.DataFrame:
    table_ref = get_table_reference(bigquery_client, dataset_id, table_id)
    table = bigquery_client.get_table(table_ref)
    return bigquery_client.list_rows(table).to_dataframe()


def get_list_from_bigquery(
        project_id: str,
        private_key_filepath: str,
        sql: str) -> list:
    """
    Returns the first column of a query as a Python list.
    """
    log_info(f'Query:\n{sql}')
    return pandas.read_gbq(
        sql,
        project_id=project_id,
        private_key=private_key_filepath,
        dialect='standard'
    ).iloc[:, 0].tolist()


def update_bigquery_view(
    bigquery_client: bigquery.Client,
        sql: str,
        dataset_id: str,
        table_id: str) -> bigquery.table.Table:
    view_ref = get_table_reference(bigquery_client, dataset_id, table_id)
    view = bigquery.Table(view_ref)
    view.view_query = sql
    try:
        view = bigquery_client.update_table(view, ['view_query'])
        log_info(
            f'{view.dataset_id}.{view.table_id} is updated.')
    except NotFound:
        view = bigquery_client.create_table(view)
        log_info(
            f'{view.dataset_id}.{view.table_id} is created since it does not exist.')
    return view


def row_iterator_to_dict_list(row_iterator: bigquery.table.RowIterator) -> List[dict]:
    return [{schema.name: row_item for schema, row_item
             in zip(row_iterator.schema, always_iterable(row.values()))}
            for row in row_iterator]
