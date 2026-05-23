from include.helpers.minio import get_minio_client
from airflow.providers.postgres.hooks.postgres import PostgresHook
import tempfile
import os

def load_to_dw(path: str):
    bucket_name = path.split('/')[0]
    object_name = '/'.join(path.split('/')[1:])

    local_csv_path = None
    response = None

    try:
        # Download CSV from MinIO to temp file
        client = get_minio_client()
        response = client.get_object(bucket_name, object_name)
        with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
            local_csv_path = tmp.name
            for chunk in response.stream(32 * 1024):
                tmp.write(chunk)

        pg_hook = PostgresHook(postgres_conn_id="postgres_conn")
        conn = pg_hook.get_conn()
        conn.autocommit = False
        cursor = conn.cursor()

        try:
            # Create real table if not exists
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS public.stock_prices_dw (
                    timestamp   TIMESTAMP NOT NULL PRIMARY KEY,
                    open        NUMERIC,
                    high        NUMERIC,
                    low         NUMERIC,
                    close       NUMERIC,
                    volume      BIGINT
                );
            """)

            # Create temp staging table
            cursor.execute("""
                CREATE TEMP TABLE staging_stock
                (LIKE public.stock_prices_dw);
            """)

            # COPY csv → staging 
            with open(local_csv_path, 'r') as f:
                cursor.copy_expert(
                    sql="COPY staging_stock FROM STDIN WITH (FORMAT CSV, HEADER TRUE)",
                    file=f
                )

            # Delete existing rows that match staging timestamps
            cursor.execute("""
                DELETE FROM public.stock_prices_dw
                WHERE timestamp IN (SELECT timestamp FROM staging_stock);
            """)

            # Batch insert all staging data 
            cursor.execute("""
                INSERT INTO public.stock_prices_dw
                SELECT * FROM staging_stock;
            """)

            # Commit — all or nothing!
            conn.commit()

        except Exception:
            conn.rollback()  # ← if anything fails, nothing saved
            raise

        finally:
            cursor.close()
            conn.close()

    finally:
        if response:
            response.close()
            response.release_conn()
        if local_csv_path and os.path.exists(local_csv_path):
            os.remove(local_csv_path)