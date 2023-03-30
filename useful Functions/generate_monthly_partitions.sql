CREATE OR REPLACE FUNCTION jet.generate_monthly_partitions(start_date date, end_date date, schema_table_name text)
 RETURNS void
 LANGUAGE plpgsql
AS $function$
DECLARE
  partition_date date := start_date;
  schema_name text;
  table_name text;
BEGIN
  schema_name := split_part(schema_table_name, '.', 1);
  table_name := split_part(schema_table_name, '.', 2);
  WHILE partition_date < end_date LOOP
    EXECUTE 'CREATE TABLE IF NOT EXISTS ' || quote_ident(schema_name) || '.' || quote_ident(table_name) ||
      '_' || to_char(partition_date, 'YYYY_MM') ||
      ' PARTITION OF ' || quote_ident(schema_name) || '.' || quote_ident(table_name) ||
      ' FOR VALUES FROM (''' || to_char(partition_date, 'YYYY-MM-DD') || ''')' ||
      ' TO (''' || to_char(date_trunc('MONTH', partition_date + INTERVAL '1 MONTH'), 'YYYY-MM-DD') || ''')';
    partition_date := date_trunc('MONTH', partition_date + INTERVAL '1 MONTH');
  END LOOP;
END;
$function$
;
