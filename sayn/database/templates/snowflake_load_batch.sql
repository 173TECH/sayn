{% set file_format = full_table_name + '_csv_format' %}
{% set stage = full_table_name + '_csv_stage' %}

CREATE OR REPLACE FILE FORMAT {{ file_format }}
  TYPE = 'CSV'
  FIELD_DELIMITER = '\t'
  SKIP_HEADER = 1
  NULL_IF = ('NaN', '0000-00-00 00:00:00', '0000-00-00')
  EMPTY_FIELD_AS_NULL = true
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  ESCAPE_UNENCLOSED_FIELD = '\\'
  ;

CREATE OR REPLACE stage {{ stage }}
  file_format = {{ file_format }};

PUT file://{{ temp_file_directory }}/{{ temp_file_name }} @{{ stage }} auto_compress=true;

COPY INTO {{ full_table_name }}
  from @{{ stage }}/{{ temp_file_name }}.gz
  file_format = (format_name = {{ file_format }});
