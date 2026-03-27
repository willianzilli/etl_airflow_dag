### model example
```sync_mode: overwrite | append
schema_change: upward_stream | all_time_stream
table: pec_grupo
columns:
  - EMPRESA
  - REVENDA
  - OPERACAO
  - DTA_OPERACAO
cursor:
  query:
    SELECT FO.DTA_OPERACAO
    FROM FAT_OPERACAO FO
    WHERE FO.DTA_OPERACAO >= :dta_operacao
  rules:
    dta_operacao:
      on_create: 2024-01-01
      on_update: ADD_MONTHS(SYSDATE, -2)
```

### params
sync_mode
  - overwrite: Overwrite by replacing pre-existing data in the destination.
  - [Future] append: Sync new records from stream and append data in destination.
  - [Future] append_deduped: Sync new records from stream and append data in destination, also provides a de-duplicated view mirroring the state of the stream in the source.

schema_change
  - upward_stream: The stream will create the new stream in the destination and fill only new inserted data.
  - all_time_stream: The first sync will create the new stream in the destination and fill all data in as if it is an initial sync.