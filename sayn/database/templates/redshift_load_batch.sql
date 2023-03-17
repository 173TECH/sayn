
copy {{ full_table_name }}
from 's3://{{ bucket }}/{{ temp_file_name }}'
iam_role default
delimiter '|'
{% if region is not none %}region '{{ region }}'{% endif %}
;
