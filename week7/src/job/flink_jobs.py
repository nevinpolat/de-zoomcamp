import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

def run_homework_streaming():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1) # Critical for single partition watermarking
    
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    # 1. Define Source (Kafka)
    t_env.execute_sql("""
        CREATE TABLE green_trips (
            lpep_pickup_datetime STRING,
            PULocationID INT,
            trip_distance DOUBLE,
            tip_amount DOUBLE,
            event_timestamp AS TO_TIMESTAMP(lpep_pickup_datetime, 'yyyy-MM-dd HH:mm:ss'),
            WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'green-trips',
            'properties.bootstrap.servers' = 'redpanda:29092',
            'properties.group.id' = 'flink-homework-v1',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json'
        )
    """)

    # 2. Define Sinks (Postgres)
    # Question 4 Sink
    t_env.execute_sql("""
        CREATE TABLE sink_q4 (
            window_start TIMESTAMP(3),
            PULocationID INT,
            num_trips BIGINT,
            PRIMARY KEY (window_start, PULocationID) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = 'green_trips_window_5m',
            'username' = 'postgres', 'password' = 'postgres'
        )
    """)

    # Question 6 Sink
    t_env.execute_sql("""
        CREATE TABLE sink_q6 (
            window_start TIMESTAMP(3),
            total_tip DOUBLE,
            PRIMARY KEY (window_start) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = 'green_trips_tips_hourly',
            'username' = 'postgres', 'password' = 'postgres'
        )
    """)

    # 3. Execute Pipelines
    # Q4: Tumbling Window 5m
    statement_q4 = """
        INSERT INTO sink_q4
        SELECT window_start, PULocationID, COUNT(*)
        FROM TABLE(TUMBLE(TABLE green_trips, DESCRIPTOR(event_timestamp), INTERVAL '5' MINUTES))
        GROUP BY window_start, PULocationID
    """
    
    # Q6: Tumbling Window 1h
    statement_q6 = """
        INSERT INTO sink_q6
        SELECT window_start, SUM(tip_amount)
        FROM TABLE(TUMBLE(TABLE green_trips, DESCRIPTOR(event_timestamp), INTERVAL '1' HOURS))
        GROUP BY window_start
    """

    statement_set = t_env.create_statement_set()
    statement_set.add_insert_sql(statement_q4)
    statement_set.add_insert_sql(statement_q6)
    statement_set.execute().wait()

if __name__ == '__main__':
    run_homework_streaming()