{% macro load_new_raw_events() %}
  copy into {{ target.database }}.raw.raw_events
    (event_id, event_type, event_timestamp, payload,
     kafka_partition, kafka_offset, ingested_at, source_file)
  from (
    select
      $1:event_id::string,
      $1:event_type::string,
      $1:event_timestamp::timestamp_tz,
      parse_json($1:payload::string),
      $1:kafka_partition::number,
      $1:kafka_offset::number,
      $1:ingested_at::timestamp_tz,
      metadata$filename
    from @{{ target.database }}.raw.raw_events_stage
  )
  on_error = 'abort_statement';
{% endmacro %}