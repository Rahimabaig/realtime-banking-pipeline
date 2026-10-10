# Real-Time Banking Data Pipeline

A local data engineering project that generates banking data in PostgreSQL, streams database changes through Debezium and Kafka, stores Parquet files in MinIO, and uses Airflow and dbt to load and model data in Snowflake.

## Architecture

```text
Fake data generator
        |
        v
PostgreSQL (customers, accounts, transactions)
        |
        | logical replication / CDC
        v
Debezium + Kafka Connect ---> Kafka topics
                                  |
                                  v
                          Kafka to MinIO consumer
                                  |
                           Parquet in MinIO
                                  |
                                  v
                         Airflow ingestion DAG
                                  |
                                  v
                         Snowflake raw tables
                                  |
                                  v
                        dbt staging and marts
```

The repository also includes a daily Airflow DAG for dbt snapshots and marts, a record-checking utility, a Power BI dashboard, and a PostgreSQL entity relationship diagram.

## Components

- **PostgreSQL** stores the source banking tables. Its Docker configuration enables logical replication for change data capture.
- **Debezium** runs as Kafka Connect and publishes changes for the three source tables.
- **Kafka** carries change events on the `banking_server.public.*` topics.
- **MinIO** stores consumer output as date-partitioned Parquet files.
- **Airflow** runs the MinIO-to-Snowflake DAG and the daily dbt snapshot/marts DAG.
- **Snowflake and dbt** provide the raw landing tables, staging views, SCD snapshots, and analytics models.

## Prerequisites

- Docker Desktop with Docker Compose
- Python 3.11 recommended for running the local Python scripts
- A Snowflake account, warehouse, and credentials for ingestion and dbt

## Configuration

Docker Compose reads variable substitutions from a root-level `.env`. The Python scripts also call `load_dotenv()`; the generator, connector registration script, and Kafka consumer expect settings in the environment or in a `.env` in the directory from which they are launched. Keep credentials private and do not commit them.

Configure these values to match your environment:

| Variables | Used for |
| --- | --- |
| `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB` | Banking PostgreSQL and Compose |
| `POSTGRES_HOST`, `POSTGRES_PORT` | Python generator and Debezium connector registration |
| `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD` | MinIO service |
| `MINIO_ENDPOINT`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`, `MINIO_BUCKET` | Python consumer and Airflow MinIO ingestion |
| `KAFKA_BOOTSTRAP`, `KAFKA_GROUP` | Kafka consumer |
| `AIRFLOW_DB_USER`, `AIRFLOW_DB_PASSWORD`, `AIRFLOW_DB_NAME` | Airflow metadata database |
| `AIRFLOW_WWW_USER_CREATE`, `AIRFLOW_WWW_USER_USERNAME`, `AIRFLOW_WWW_USER_PASSWORD`, `AIRFLOW_WWW_USER_FIRSTNAME`, `AIRFLOW_WWW_USER_LASTNAME`, `AIRFLOW_WWW_USER_EMAIL`, `AIRFLOW_WWW_USER_ROLE` | Airflow web user initialization |
| `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`, `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_WAREHOUSE`, `SNOWFLAKE_DB`, `SNOWFLAKE_SCHEMA` | Airflow Snowflake ingestion |
| Snowflake profile values | dbt profile at `banking_dbt/.dbt/profiles.yml` |

Compose publishes Kafka at port 29092 for host access and 9092 on the Docker network. Set `KAFKA_BOOTSTRAP` to the broker address reachable from the consumer. Connector registration calls Kafka Connect at `http://localhost:8083`.

## Run locally

### 1. Start the services

From the repository root, after configuring `.env`:

```bash
docker compose up -d --build
docker compose ps
```

The stack includes PostgreSQL, Kafka, ZooKeeper, Kafka Connect, MinIO, and Airflow with its metadata database.

- Airflow: http://localhost:8080
- MinIO console: http://localhost:9001
- Kafka Connect REST API: http://localhost:8083

### 2. Create the source tables

Apply the schema to the banking PostgreSQL database:

```bash
psql -h localhost -p 5432 -U <POSTGRES_USER> -d <POSTGRES_DB> -f postgres/schema.sql
```

Install Python dependencies for host-run scripts:

```bash
python -m pip install -r requirements.txt
```

### 3. Generate sample data

Run from the project root with the PostgreSQL environment variables set:

```bash
python data-generator/fake_generator.py --once
```

The generator creates 10 customers, two accounts per customer, and 50 transactions per iteration. Without `--once`, it repeats every two seconds until stopped.

### 4. Register Debezium

After Kafka Connect is available and source tables exist, run:

```bash
python kafka-debezium/postgres_connector.py
```

The connector captures changes from `public.customers`, `public.accounts`, and `public.transactions`.

### 5. Start the Kafka-to-MinIO consumer

In a separate terminal, with Kafka and MinIO settings configured:

```bash
python consumers/kafka_to_minio.py
```

The consumer buffers up to 50 row images per topic and writes Parquet files under table and date prefixes in the configured bucket.

### 6. Load and model data

In Airflow, trigger the `minio_to_snowflake_banking` DAG to download Parquet objects and load them into Snowflake raw tables. Create the raw tables first and align their database/schema with `banking_dbt/models/staging/sources.yml`.

The `SCD2_snapshots` DAG runs dbt snapshots daily and then builds marts. For manual dbt work, configure the `banking_dbt` profile in `banking_dbt/.dbt/profiles.yml`, then run:

```bash
cd banking_dbt
dbt deps
dbt snapshot --profiles-dir .dbt
dbt run --profiles-dir .dbt
dbt test --profiles-dir .dbt
```

The dbt project includes staging models for customers, accounts, and transactions; customer and account snapshots; and customer/account dimensions plus a transaction fact model.

## Repository layout

```text
.
��y��yۧu���@�w^~)�t postgres/schema.sql                 # PostgreSQL source schema�u���\5�@���ф�����ɅѽȽ����}����Ʌѽȹ�䀀����M�ѡ�ѥ����������ɕ��ɑ�+�u���n�ן�w���y��yЁ�����������մ����������������������������մ�������ѽȁɕ����Ʌѥ��(֍�����յ��̼����������������������������-���������յ�ȁ����5��%<��������(֍������Ƚ���̼���������������������������ə��܁�����ѥ���������Ё�+�u���n�ן�w���y��yЁ�������}��м����������������������������Ёͽ�ɍ�̰��х������͹��͡��̰������+�u���n�ן�w���y��yЁ�����ȵ�����͔�嵰��������������������1�����͕�٥����х��(֍��������}�ɐ�)A������������������������ѥ��ɕ��ѥ��͡�������Ʌ�+�u���n�ן�w���y��yЁ�������}��͡���ɐ����������������������A�ݕȁ	$���͡���ɐ)���((���
$�
()��!Ո��ѥ��́ݽɭ����́�ɔ��������ѡՈ�ݽɭ����̀��
$����х��́�����������̰��չ́Iՙ�����ѕ���́��ѕ�Ё���ѕ��́�ɔ��ɕ͕�а������������́��и�
��չ́��Ё������ЁM��ݙ�����ݡ���������́�ɔ���͡���Ѽ���������
������ɔ�ѡ��M��ݙ�����͕�ɕ�́ɕ��ɕ�������ѡ��ݽɭ����́���ɕ��ͥѽ��͕�ѥ��́����ɔ�ɕ�她�����ѡ�͔����̸((���9�ѕ�((��Q�������Ʌѽȁ�ɕ�ѕ́��ѡ�ѥ����ٕ������Ё��ф�(��Q���5��%<�����յ�ȁ�ɥѕ́��A���ՕЁ�����ݡ�����ѽ�����ՙ��ȁɕ����̀���ɕ��ɑ̸�Q������ɕ�Ё͍ɥ�Ё���́��Ё���͠�͵����ȁ��������э��́ݡ����Ё�ѽ�̸(��Q����ə��܁�����ȁ������́M��ݙ�����Ʌ܁х���́������ѕɹ���х�����х��́Ѽ����и�����ѝɕ͍̽������ű���ɕ�ѕ́����ѡ��A��ѝɕME0�ͽ�ɍ��х���̸(��!��ѹ���́����������ݡ�ɔ�͍ɥ��́�ո聥�ͥ�������ȁ�͔�
����͔�͕�٥��������쁙ɽ��ѡ�����Ё�͔��Չ��͡�������́�������еɕ�����������ɕ�͕�
