import dlt

@dlt.table()
def DimUserStg():
    df = spark.readStream.table('spotify_prj.silver.dimuser')
    return df


dlt.create_streaming_table('dimuser')

dlt.create_auto_cdc_flow(
  target = "dimuser",
  source = "DimUserStg",
  keys = ["user_id"],
  sequence_by = "updated_at",
  stored_as_scd_type = 2,
  track_history_except_column_list = None,
  name = None,
  once = False
)