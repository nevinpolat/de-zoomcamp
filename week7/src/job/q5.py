import os
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

def run_session_job():
    # 1. Environment Setup
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(environment_settings=settings)
    
    # Force single parallelism to ensure deterministic watermark behavior
    t_env.get_config().set("parallelism.default", "1")

    # 2. Source Table (Green Trips)
    # Note: We use TO_TIMESTAMP and ensure the Watermark is robust
    source_ddl = """
        CREATE TABLE green_trips (
            PULocationID INT,
            lpep_pickup_datetime STRING,
            row_time AS TO_TIMESTAMP(lpep_pickup_datetime, 'yyyy-MM-dd HH:mm:ss'),
            WATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'green-trips',
            'properties.bootstrap.servers' = 'redpanda:29092',
            'properties.group.id' = 'flink-session-hw-final',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json'
        )
    """
    
    # 3. Sink Table (Postgres)
    sink_ddl = """
        CREATE TABLE session_stats (
            PULocationID INT,
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            trip_count BIGINT,
            PRIMARY KEY (PULocationID, window_start) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = 'session_stats',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        )
    """

    t_env.execute_sql(source_ddl)
    t_env.execute_sql(sink_ddl)

    # 4. Session Window Logic
    # SESSION(time_col, gap_interval)
    # We select the count to find that "Longest Session"
    t_env.execute_sql("""
        INSERT INTO session_stats
        SELECT 
            PULocationID,
            SESSION_START(row_time, INTERVAL '5' MINUTES) as window_start,
            SESSION_END(row_time, INTERVAL '5' MINUTES) as window_end,
            COUNT(1) as trip_count
        FROM green_trips
        GROUP BY 
            PULocationID, 
            SESSION(row_time, INTERVAL '5' MINUTES)
    """).wait()

if __name__ == '__main__':
    run_session_job()