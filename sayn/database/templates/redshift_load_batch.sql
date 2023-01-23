
copy {{full_table_name}}
from 's3://{{bucket}}/{{temp_file_name}}'
iam_role '{{arn}}'
delimiter '|';
