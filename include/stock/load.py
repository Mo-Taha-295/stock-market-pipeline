from io import StringIO
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook


def load_to_dw(path: str) -> None:
    """
        Load stock data from MinIO into PostgreSQL DW using staging + UPSERT

    """

    bucket_name, key = path.split("/", 1)

    # Read CSV from MinIO
    s3_hook = S3Hook(aws_conn_id="minio_conn")

    csv_content = s3_hook.read_key(
        key=key,
        bucket_name=bucket_name
    )

    csv_buffer = StringIO(csv_content)

    pg_hook = PostgresHook(postgres_conn_id="postgres_conn")
    conn = pg_hook.get_conn()

    try:
        with conn.cursor() as cursor:

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS public.stock_prices_dw (
                    timestamp TIMESTAMP PRIMARY KEY,
                    open NUMERIC,
                    high NUMERIC,
                    low NUMERIC,
                    close NUMERIC,
                    volume BIGINT
                );
            """)

            cursor.execute("""
                CREATE TEMP TABLE staging_stock
                (LIKE public.stock_prices_dw);
            """)

            cursor.copy_expert(
                sql="""
                    COPY staging_stock
                    FROM STDIN
                    WITH (
                        FORMAT CSV,
                        HEADER TRUE
                    )
                """,
                file=csv_buffer
            )

            cursor.execute("""
                INSERT INTO public.stock_prices_dw (
                    timestamp,
                    open,
                    high,
                    low,
                    close,
                    volume
                )
                SELECT
                    timestamp,
                    open,
                    high,
                    low,
                    close,
                    volume
                FROM staging_stock

                ON CONFLICT (timestamp)
                DO UPDATE SET
                    open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    volume = EXCLUDED.volume;
            """)

        conn.commit()

    except Exception:
        conn.rollback()
        raise

    finally:
        conn.close()

        