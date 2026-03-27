# pylint: disable=missing-module-docstring
# pylint: disable=missing-class-docstring
# pylint: disable=missing-function-docstring
# pylint: disable=import-outside-toplevel
# pylint: disable=no-value-for-parameter
# pylint: disable=too-many-locals
import os
import sys
import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.models import Param
from airflow.models.baseoperator import chain
from airflow.utils.trigger_rule import TriggerRule

import oracledb
sys.modules["cx_Oracle"] = oracledb
sys.modules["cx_Oracle"].version = "8.3.0"

with DAG(
    dag_id='etl',
    schedule="0 0 * * 1-5",
    start_date=pendulum.datetime(2025, 4, 17, 0, 0, 0, tz='America/Sao_Paulo'),
    tags=['etl'],
    params={
        'conn_id': Param(
            title="Connection ID",
            default="ORACLE_HOOK",
            type="string"
        ),
        'schema': Param(
            title="Origin schema",
            default="DEAULT_SCHEMA",
            type="string"
        ),
        'schema_repl': Param(
            title="Postgres Warehouse Schema",
            default="warehouse",
            type="string"
        )
    }
) as dag:
    sys.path.append(dag.folder)
    dag.doc_md = open(os.path.join(dag.folder, 'README.md'), encoding="utf-8").read()
    model_dir = os.path.join(dag.folder, "models/")

    def setup_dag():
        models_enum = ["ALL"]
        models_value_display = {
            "ALL": "ALL"
        }

        for file in os.listdir(model_dir):
            if (file.endswith('.yaml') or file.endswith('.yml')):
                models_enum.append(file)
                models_value_display.update({
                    file: os.path.splitext(file)[0].upper()
                })

        dag.params['model'] = Param(
            title="Model to run",
            default="ALL",
            type="string",
            enum=models_enum,
            values_display=models_value_display
        )
    setup_dag()

    @task()
    def get_models(params):
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        from sqlalchemy import text
        from sqlalchemy.engine import Engine, CursorResult

        import hashlib
        import json
        import yaml

        warehouse:Engine = PostgresHook('WAREHOUSE_BRONZE').get_sqlalchemy_engine()

        models_list = []
        for file in os.listdir(model_dir):
            if (
                (params['model'] == 'ALL' or params['model'] == file)
                and (file.endswith('.yaml') or file.endswith('.yml'))
            ):
                filename = os.path.join(model_dir, file)

                hash_object = hashlib.sha256()

                with open(filename, 'r', encoding='utf-8') as file:
                    model_data = yaml.safe_load(file)
                    hash_object.update(json.dumps(file.read(), sort_keys=True).encode('utf-8'))

                model_data['filename'] = filename
                model_data['origin'] = str(params['schema_repl']).lower()
                model_data['checksum'] = hash_object.hexdigest()
                model_data['columns'] = [str(column).lower() for column in model_data['columns']]

                del hash_object

                cursor:CursorResult = warehouse.execute(
                    text("""
                    SELECT last_sync
                    FROM public.internal
                    WHERE origin = :origin
                        AND "table" = :table
                    """),
                    **{
                        'origin': model_data['origin'],
                        'table': model_data['table']
                    }
                )
                model_from_db = cursor.fetchone()

                model_data['last_sync'] = model_from_db['last_sync'] if model_from_db else None

                models_list.append(model_data)

        return models_list

    @task(multiple_outputs=True)
    def check_structuries(models_list, params) -> dict[str, list[dict]]:
        import hashlib
        import json

        from airflow.providers.postgres.hooks.postgres import PostgresHook
        from sqlalchemy.engine import Engine

        import pandas as pd

        warehouse:Engine = PostgresHook('WAREHOUSE_BRONZE').get_sqlalchemy_engine()

        df_internal:pd.DataFrame = pd.read_sql_query(
            con=warehouse,
            sql="""
                select "table", checksum
                from public.internal
                where origin = %(origin)s
            """,
            params={
                'origin': str(params['schema_repl']).lower()
            }
        )

        struct = {
            'setup': [],
            'update': []
        }
        for model in models_list:
            if model['table'] not in df_internal['table'].to_list():
                struct['setup'].append(model)
            else:
                hash_object = hashlib.sha256()

                with open(
                    os.path.join(dag.folder, 'models/', model['filename'])
                    , 'r'
                    , encoding='utf-8'
                ) as file:
                    hash_object.update(json.dumps(file.read(), sort_keys=True).encode('utf-8'))

                checksum = hash_object.hexdigest()
                if (model['checksum'] != checksum):
                    struct['update'].append(model)

                print(model['checksum'], '->', checksum)

                del checksum

        return struct

    @task(max_active_tis_per_dag=1)
    def setup_structuries(model:dict, params):
        from airflow.providers.oracle.hooks.oracle import OracleHook
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        from sqlalchemy import insert, inspect, MetaData, Table, Column
        from sqlalchemy.engine import Engine
        from sqlalchemy.engine.reflection import Inspector
        from sqlalchemy.exc import ProgrammingError, IntegrityError
        from sqlalchemy.schema import CreateSchema, PrimaryKeyConstraint, UniqueConstraint

        from sqlalchemy.dialects.oracle import NUMBER
        from sqlalchemy.dialects.postgresql import NUMERIC

        from psycopg2 import errors
        from psycopg2.errorcodes import UNIQUE_VIOLATION, CHECK_VIOLATION

        import pandas as pd

        def explode_list(data, column):
            df = pd.DataFrame(data)

            if df.empty:
                return []

            columns_list = df[column].explode()

            return columns_list.drop_duplicates().tolist()

        print(model.get('origin'), model.get('table'), model.get('filename'))

        origin:Engine = OracleHook(params['conn_id']).get_sqlalchemy_engine()
        warehouse:Engine = PostgresHook('WAREHOUSE_BRONZE').get_sqlalchemy_engine()

        ### INSERT MODEL INFORMATION IN CONFIGURATION TABLE
        metadata_internal = MetaData(schema='public')
        table_internal = Table('internal', metadata_internal, autoload_with=warehouse)

        try:
            if 'cursor' in model:
                del model['cursor']

            insert_stmt = insert(table_internal).values(**model)
            warehouse.execute(insert_stmt)
        except IntegrityError as e:
            if isinstance(e.orig, errors.lookup(CHECK_VIOLATION)):
                raise e
            elif isinstance(e.orig, errors.lookup(UNIQUE_VIOLATION)):
                print(str(e))
                return
            else:
                print(str(e))

        ### GET TABLE DLL FROM ORIGINAL DATABASE
        metadata = MetaData(schema=params['schema'])
        table = Table(model.get('table'), metadata, autoload_with=origin)

        inspector:Inspector = inspect(origin)

        primary_keys = inspector.get_pk_constraint(model.get('table'))
        foreign_keys = explode_list(
            data=inspector.get_foreign_keys(model.get('table')),
            column='constrained_columns'
        )
        indexes = explode_list(
            data=inspector.get_indexes(model.get('table')),
            column='column_names'
        )
        unique_constaint = explode_list(
            data=inspector.get_unique_constraints(model.get('table')),
            column='column_names'
        )

        ### GENERATE TABLE CREATION DDL
        metadata_repl = MetaData(schema=params['schema_repl'])
        table_repl = Table(model.get('table'), metadata_repl)

        for column in table.columns:
            if (
                column.name in model['columns']
                or column.name in primary_keys['constrained_columns']
                or column.name in foreign_keys
                or column.name in indexes
                or column.name in unique_constaint
            ):
                column_type = column.type

                if isinstance(column.type, NUMBER):
                    column_type = NUMERIC(
                        precision=column.type.precision,
                        scale=column.type.scale
                    )

                table_repl.append_column(
                    Column(
                        name=column.name,
                        type_=column_type
                    )
                )

        table_repl.append_constraint(PrimaryKeyConstraint(*primary_keys['constrained_columns']))

        if len(unique_constaint) > 0:
            table_repl.append_constraint(UniqueConstraint(*unique_constaint))

        ### CREATE TABLE
        try:
            warehouse.execute(CreateSchema(params['schema_repl']))
        except ProgrammingError as e:
            print(str(e))

        ### CREATE TABLE
        metadata_repl.create_all(warehouse)

    @task(max_active_tis_per_dag=1)
    def update_structuries(model:dict, params):
        from airflow.exceptions import AirflowSkipException
        raise AirflowSkipException("IN DEVELOPMENT")

        # from airflow.providers.oracle.hooks.oracle import OracleHook
        # from airflow.providers.postgres.hooks.postgres import PostgresHook
        # from sqlalchemy import MetaData, Table, Column, inspect
        # from sqlalchemy.engine import Engine
        # from sqlalchemy.engine.reflection import Inspector

        # import yaml
        # import hashlib
        # import pandas as pd

        # warehouse:Engine = PostgresHook('WAREHOUSE_BRONZE').get_sqlalchemy_engine()

        # df = pd.read_sql_query(
        #     con=warehouse,
        #     sql="select * from public.internal where last_sync is not null"
        # )
        # origin:Engine = OracleHook(params['conn_id']).get_sqlalchemy_engine()
        # inspector:Inspector = inspect(origin)
        # metadata = MetaData(schema=params['schema'])

        # metadata_repl = MetaData(schema=params['schema_repl'])
        # ddl_repl = Table(row.table, metadata_repl, autoload_with=warehouse)

        # for row in df.itertuples():
        #     with open(row.filename, 'r', encoding='utf-8') as file:
        #         model_yaml = yaml.safe_load(file)

        #     hash_object = hashlib.sha256()
        #     hash_object.update(model_yaml)
        #     checksum = hash_object.hexdigest()

        #     if row.checksum != checksum:
        #         old_model = pd.DataFrame(row.model)
        #         new_model = pd.DataFrame(model_yaml)
        #         new_model = new_model.drop(columns=['columns']).drop_duplicates()

        #         ddl = Table(row.table, metadata, autoload_with=origin)
        #         metadata.schema = row.origin
        #         metadata.tables[row.table] = ddl

        #         new_columns = pd.DataFrame(inspector.get_columns())
        #         new_columns = new_columns[
        #             new_columns['name'].isin(new_model['colums'])
        #             & ~new_columns['name'].isin(old_model['colums'])
        #         ]

        #         for column in new_columns:
        #             ddl_repl.append_column(
        #                 Column(column.name, column.type.compile(warehouse.dialect))
        #             )

        #         metadata_repl.create_all(warehouse, checkfirst=True)

    @task(max_active_tis_per_dag=1, trigger_rule=TriggerRule.ALL_SUCCESS or TriggerRule.ALL_SKIPPED)
    def overwrite_all_time_stream(model:dict, params):
        from airflow.providers.oracle.hooks.oracle import OracleHook
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        from sqlalchemy import Table, MetaData, text, select
        from sqlalchemy.engine import Engine, CursorResult
        from sqlalchemy.orm import sessionmaker

        print(model.get('origin'), model.get('table'), model.get('filename'))

        chunksize = 1000

        origin:Engine = OracleHook(params['conn_id']).get_sqlalchemy_engine()
        warehouse:Engine = PostgresHook('WAREHOUSE_BRONZE').get_sqlalchemy_engine()
        session = sessionmaker(bind=warehouse)()

        warehouse.execute(text(f"TRUNCATE {params['schema_repl']}.{model.get('table')}"))

        metadata = MetaData(schema=params['schema'])
        table = Table(model.get('table'), metadata, autoload_with=origin)

        metadata_repl = MetaData(schema=params['schema_repl'])
        table_repl = Table(model.get('table'), metadata_repl, autoload_with=warehouse)

        with origin.connect() as connection:
            select_stmt = select(
                *[
                    getattr(table.columns, str(column).lower())
                    for column in model.get('columns')
                ]
            )
            result:CursorResult = connection.execution_options(
                stream_results=True
            ).execute(select_stmt)

            while True:
                chunk = result.fetchmany(chunksize)

                if not chunk:
                    break

                chunk_data = [dict(row) for row in chunk]

                session.execute(table_repl.insert(), chunk_data)
                session.commit()
        session.close()

        with warehouse.connect() as connection:
            result = connection.execute(
                statement=text("""
                    UPDATE public.internal
                    SET last_sync = current_timestamp
                    WHERE origin = :origin
                        AND "table" = :table
                """),
                **{
                    'origin': params['schema_repl'],
                    'table': model.get('table')
                }
            )

        origin.dispose()
        warehouse.dispose()

    @task(max_active_tis_per_dag=1, trigger_rule=TriggerRule.ALL_SUCCESS or TriggerRule.ALL_SKIPPED)
    def overwrite_upward_stream(model:dict, params):
        import re

        from airflow.providers.oracle.hooks.oracle import OracleHook
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        from sqlalchemy import Table, MetaData, text
        from sqlalchemy.dialects.postgresql import insert
        from sqlalchemy.engine import Engine, CursorResult
        from sqlalchemy.exc import NoSuchColumnError
        from sqlalchemy.orm import sessionmaker

        print(model.get('origin'), model.get('table'), model.get('filename'))

        def is_sql_command(value):
            # Define a regular expression pattern to match SQL commands and functions with
            # parentheses
            ora_syntax = r'^\s*(SELECT|INSERT|UPDATE|DELETE|INTERVAL|SYSDATE|[A-Z_]+\(.*\))\s*$'
            sql_pattern = re.compile(ora_syntax, re.IGNORECASE)
            return bool(sql_pattern.match(value))

        def proccess_stmt_params(action, rules:dict):
            params = {}
            query_formating = {}
            for key, value in rules.items():
                data = value[action]

                if (isinstance(data, str) or is_sql_command(data)):
                    query_formating[key] = data
                else:
                    params[key] = data

            return params, query_formating

        chunksize = 1000

        origin:Engine = OracleHook(params['conn_id']).get_sqlalchemy_engine()
        warehouse:Engine = PostgresHook('WAREHOUSE_BRONZE').get_sqlalchemy_engine()
        session = sessionmaker(bind=warehouse)()

        metadata_repl = MetaData(schema=params['schema_repl'])

        table_repl = Table(model.get('table'), metadata_repl, autoload_with=warehouse)
        primary_keys = [key.name for key in table_repl.primary_key]
        on_conflict_columns = [
            col.name
            for col in table_repl.columns
                if col.name not in primary_keys
        ]

        stmt_params, query_formating = proccess_stmt_params(
            action='on_update',
            rules = model['cursor']['rules']
        )

        if model.get('last_sync') is None:
            with warehouse.begin() as connection:
                # Lock the table
                connection.execute(text(f"""
                    LOCK TABLE {params['schema_repl']}.{model.get('table')}
                    IN EXCLUSIVE MODE
                """))

                # Truncate the table
                connection.execute(table_repl.delete())

            stmt_params, query_formating = proccess_stmt_params(
                action='on_create',
                rules = model['cursor']['rules']
            )

        sql_statement = model['cursor']['query']
        alias_match_pattern = r"FROM\s+\S+\s+(?:AS\s)?(\w+)(?=\s+(WHERE|JOIN|LEFT|RIGHT|OUTER|$))"
        alias_match = re.search(alias_match_pattern, sql_statement)
        projection = model['columns']
        if alias_match:
            table_alias = alias_match.group(1)
            projection = [f"{table_alias}.{col}" for col in model['columns']]

        sql_statement = str(sql_statement).replace("*", ", ".join(projection))

        for key, value in query_formating.items():
            sql_statement = str(sql_statement).replace(f":{key}", value)

        print("[SQL: ", sql_statement, "]")
        print("[parameters: ", stmt_params, "]")

        with origin.connect() as connection:
            result:CursorResult = connection.execution_options(
                stream_results=True
            ).execute(
                statement=text(sql_statement),
                **stmt_params
            )

            while True:
                chunk = result.fetchmany(chunksize)

                if not chunk:
                    break

                chunk_data = [dict(row) for row in chunk]

                insert_stmt = insert(table_repl).values(chunk_data)

                if model.get('last_sync'):
                    on_conflict_set = dict()
                    for column in on_conflict_columns:
                        try:
                            if column not in primary_keys:
                                on_conflict_set[column] = insert_stmt.excluded[column]
                        except KeyError as e:
                            raise NoSuchColumnError(str(e)) from e

                    insert_stmt = insert_stmt.on_conflict_do_update(
                        index_elements=primary_keys,
                        set_=on_conflict_set
                    )

                session.execute(insert_stmt)
                session.commit()

        session.execute(
            statement=text("""
                UPDATE public.internal
                SET last_sync = current_timestamp
                WHERE origin = :origin
                    AND "table" = :table
            """),
            params={
                'origin': params['schema_repl'],
                'table': model.get('table')
            }
        )
        session.commit()
        session.close()
        origin.dispose()
        warehouse.dispose()

    @task(trigger_rule=TriggerRule.NONE_FAILED)
    def dummy_all_concluded():
        pass

    @task
    def dummy_xcom_map(xcom, key):
        return xcom[key]

    @task
    def map_dynamic_tasks(model_list) -> dict[str, list[dict]]:
        tasks = {
            'overwrite_all_time_stream': [],
            'overwrite_upward_stream': []
        }

        for model in model_list:
            tasks[f"{model['sync_mode']}_{model['schema_change']}"].append(model)

        return tasks

    models = get_models()
    structure = check_structuries(models)

    setup_structure = dummy_xcom_map.override(
        task_id="dummy_get_structure_setup"
    )(structure, "setup")
    tk_setup_structuries = setup_structuries.expand(model=setup_structure)

    update_structure = dummy_xcom_map.override(
        task_id="dummy_get_structure_update"
    )(structure, "update")
    tk_update_structuries = update_structuries.expand(model=update_structure)

    tk_dummy_all_concluded = dummy_all_concluded()

    dynamic_tasks = map_dynamic_tasks(model_list=models)

    mapped_overwrite_all_time_stream_tasks = dummy_xcom_map.override(
        task_id="dummy_get_mapped_task_overwrite_all_time_stream"
    )(dynamic_tasks, "overwrite_all_time_stream")
    tk_overwrite_all_time_stream = overwrite_all_time_stream.expand(
        model=mapped_overwrite_all_time_stream_tasks
    )

    mapped_overwrite_upward_stream_tasks = dummy_xcom_map.override(
        task_id="dummy_get_mapped_overwrite_upward_stream_task"
    )(dynamic_tasks, "overwrite_upward_stream")
    tk_overwrite_upward_stream = overwrite_upward_stream.expand(
        model=mapped_overwrite_upward_stream_tasks
    )

    chain(
        models,
        structure,
        [setup_structure, update_structure],
        [tk_setup_structuries, tk_update_structuries],
        tk_dummy_all_concluded,
        dynamic_tasks,
        [mapped_overwrite_all_time_stream_tasks, mapped_overwrite_upward_stream_tasks],
        [tk_overwrite_all_time_stream, tk_overwrite_upward_stream]
    )
