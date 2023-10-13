from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, col, concat_ws, to_timestamp, when, explode, mean, count, window, sum
from schemas import ts_event_schema
from schedule import get_schedules


def expand_location_col(df: DataFrame, col: str):
    # Pull out first location item from array
    original_col_name = 'ns5:' + col

    # Expand the items inside the struct
    df_expanded = df.select(df.columns + [original_col_name + '.*'])

    # Convert timestamps
    for time_col in ['@at', '@et', '@wet']:
        df_expanded = convert_timestamp(df_expanded, '@ssd', time_col)

    # Rename each inner column to add the col name as a prefix
    columns_to_rename = ['at', 'et', 'wet', 'src', 'delayed'] # col names without @ prefix
    df_renamed = df_expanded
    for col_to_rename in columns_to_rename:
        df_renamed = df_renamed.withColumnRenamed('@' + col_to_rename, col + '_' + col_to_rename)

    # drop the temp col we added and the original col
    return df_renamed.drop(original_col_name)


# Converts a string of the format HH:mm to a timestamp by combining with the specified date col (yyyy-dd-MM) and parsing
def convert_timestamp(df: DataFrame, date_col: str, time_col: str):
    df = df.withColumn(time_col, when(df[time_col].isNotNull(), concat_ws(' ', date_col, time_col)).otherwise(None))
    df = df.withColumn(time_col, to_timestamp(time_col, 'yyyy-MM-dd HH:mm'))
    return df


def format_ts_events(df_raw: DataFrame):
    # Parse JSON
    df_json_str = df_raw.select(
        from_json(col("value"), ts_event_schema).alias("json")
    )

    # Explode out the JSON
    df_json = df_json_str.select('json.*')

    # Location column is an array so explode it out into separate rows
    df_with_location_col = (df_json
                            .withColumn('location', explode('ns5:Location'))
                            .select(df_json.columns + ['location.*']))

    # Convert timestamps
    for time_col in ['@ptd', '@pta']:
        df_with_location_col = convert_timestamp(df_with_location_col, '@ssd', time_col)

    # Drop any rows with a pass column, as we are only interested in stops
    df_with_location_col = df_with_location_col.filter(df_with_location_col['ns5:pass'].isNull()).drop('ns5:pass')

    df_final = df_with_location_col
    location_cols = ['arr', 'dep']
    for location_col in location_cols:
        df_final = expand_location_col(df_final, location_col)

    # remove the @ prefix from column names
    cols_to_rename = filter(lambda x: x[0] == '@', df_final.columns)
    for col_name in cols_to_rename:
        df_final = df_final.withColumnRenamed(col_name, col_name[1:])

    return df_final


def get_schedules_df(spark: SparkSession):
    schedules_raw = get_schedules()
    df = spark.createDataFrame(schedules_raw)

    for time_col in ['pta', 'ptd']:
        df = convert_timestamp(df, 'ssd', time_col)

    return df


def diff_minutes(col_a, col_b):
    seconds = col(col_a).cast("long") - col(col_b).cast("long")
    return seconds / 60


TOPIC_NAME = 'ts-events'
spark = SparkSession.builder.appName('sdpnr').getOrCreate()


spark.sparkContext.setLogLevel("WARN")

# Subscribe to kafka topic
# df = spark \
#     .readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "localhost:9092") \
#     .option("subscribe", TOPIC_NAME) \
#     .load()

# Subscribe to events on socket
df = spark \
    .readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 5555) \
    .load()

df_raw = df.selectExpr("CAST(value as STRING)")

# Format incoming Train Status events
df_ts_events = format_ts_events(df_raw).select('rid', 'uid', 'ssd', 'tpl', 'pta', 'ptd', 'arr_at', 'dep_at')

# Only care about events which have actual departure times
df_ts_events = df_ts_events.na.drop(how='all', subset=['dep_at'])

# # Add columns for delay and boolean yes/no was late
events_with_delay = (df_ts_events
                     .withColumn('dep_delay_mins', diff_minutes('dep_at', 'ptd'))
                     .withColumn('late', when(col('dep_delay_mins') > 1, True).otherwise(False)))

ptd_window = window('ptd', "1 hour", "1 minute")

# Aggregate events to get number of late departures and average delay of those late departures
df_deps_by_tpl = events_with_delay.groupBy(ptd_window, 'tpl')
df_deps_by_tpl = df_deps_by_tpl.agg(
    mean('dep_delay_mins').alias('avg_dep_delay_mins'),
    sum(col('late').cast('long')).alias('late_count'),
    count('*').alias('actual_count')
)

# Aggregate schedule to get total number of departures per time window
df_schedule = get_schedules_df(spark)
df_windowed_schedule = df_schedule.groupBy(ptd_window, 'tpl')
df_agg_schedule = df_windowed_schedule.agg(count('*').alias('planned_count'))

# Combine aggregated schedule with late departures
df_joined = df_agg_schedule.join(df_deps_by_tpl, ['window', 'tpl'], 'inner')
df_results = df_joined.withColumn('total_avg_delay_mins', col('late_count') * col('avg_dep_delay_mins') / col('planned_count'))

# Filter to manchester piccadilly
df_results_mcr = df_results.where("tpl = 'MNCRPIC'")

# TODO: handle duplicate events / subsequent events for the same tpl/rid - can safely dedeupe once we have only actual deps

query = df_results_mcr.writeStream.outputMode('complete').format("console").start()
query.awaitTermination()

