from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, DataTypes, TableEnvironment, StreamTableEnvironment
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.time import Duration

def create_trip_events_sink(t_env):
    table_name = 'green_trips'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
        session_start TIMESTAMP,
        session_end TIMESTAMP,
        pickup_timestamp TIMESTAMP,
        dropoff_timestamp TIMESTAMP,
        PULocationID INTEGER,
        DOLocationID INTEGER,
        passenger_count DOUBLE,
        trip_distance DOUBLE,
        tip_amount DOUBLE
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = '{table_name}',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        );
        """
    t_env.execute_sql(sink_ddl)
    return table_name


def create_events_source_kafka(t_env):
    table_name = "trip_events"
    source_ddl = f"""
        CREATE TABLE {table_name} (
            lpep_pickup_datetime BIGINT,
            lpep_dropoff_datetime BIGINT,
            PULocationID INTEGER,
            DOLocationID INTEGER,
            passenger_count DOUBLE,
            trip_distance DOUBLE,
            tip_amount DOUBLE,
            pickup_timestamp AS TO_TIMESTAMP(FROM_UNIXTIME(lpep_pickup_datetime / 1000)),  -- Convert to TIMESTAMP
            dropoff_timestamp AS TO_TIMESTAMP(FROM_UNIXTIME(lpep_dropoff_datetime / 1000)), -- Convert to TIMESTAMP
            WATERMARK FOR dropoff_timestamp AS dropoff_timestamp - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'redpanda-1:29092',
            'topic' = 'green-trips',
            'scan.startup.mode' = 'earliest-offset',
            'properties.auto.offset.reset' = 'earliest',
            'format' = 'json',
            'json.fail-on-missing-field' = 'false',
            'json.ignore-parse-errors' = 'true'
        );
    """
    t_env.execute_sql(source_ddl)
    return table_name



def log_processing():
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)  # 10 seconds
    env.set_parallelism(3)

    # Set up the table environment
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    try:
        # Create Kafka table
        source_table = create_events_source_kafka(t_env)
        sink_table = create_trip_events_sink(t_env)

        t_env.execute_sql(
            f"""
                    INSERT INTO {sink_table}
                    SELECT
                        SESSION_START(dropoff_timestamp, INTERVAL '5' MINUTE) AS session_start,
                        SESSION_END(dropoff_timestamp, INTERVAL '5' MINUTE) AS session_end,
                        pickup_timestamp,
                        dropoff_timestamp,
                        PULocationID,
                        DOLocationID,
                        passenger_count,
                        trip_distance,
                        tip_amount
                                 
                    FROM {source_table}
                    GROUP BY
                            SESSION(dropoff_timestamp, INTERVAL '5' MINUTE), 
                            pickup_timestamp, 
                            dropoff_timestamp, 
                            PULocationID, 
                            DOLocationID, 
                            passenger_count, 
                            trip_distance, 
                            tip_amount
                    """
        ).wait()

    except Exception as e:
        print("Writing records from Kafka to JDBC failed:", str(e))


if __name__ == '__main__':
    log_processing()
