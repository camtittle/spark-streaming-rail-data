from pyspark.sql.types import *

ts_event_location_time_schema = StructType([
    StructField("@at", StringType()),
    StructField("@et", StringType()),
    StructField("@wet", StringType()),
    StructField("@src", StringType()),
    StructField("@delayed", BooleanType()),
])

ts_event_location_schema = StructType([
    StructField("@tpl", StringType()),
    StructField("@wtp", StringType()),
    StructField("@wta", StringType()),
    StructField("@wtd", StringType()),
    StructField("@pta", StringType()),
    StructField("@ptd", StringType()),
    # StructField("ns5:plat", StringType()),
    StructField("ns5:pass", ts_event_location_time_schema),
    StructField("ns5:arr", ts_event_location_time_schema),
    StructField("ns5:dep", ts_event_location_time_schema),

])

ts_event_schema = StructType([
    StructField("@rid", StringType()),
    StructField("@uid", StringType()),
    StructField("@ssd", StringType()),
    StructField("ns5:Location", ArrayType(ts_event_location_schema))
])