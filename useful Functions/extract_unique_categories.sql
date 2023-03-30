CREATE OR REPLACE FUNCTION jet.extract_text_in_quotes(input_array text)
 RETURNS text
 LANGUAGE plpgsql
AS $function$
BEGIN
  RETURN (
    SELECT string_agg('''' || a.str || '''',',' ORDER BY a.str)
    FROM (
      SELECT DISTINCT replace(unnest(string_to_array(replace(replace(input_array,'[',''),']',''),''', ''')),'''','') AS str
    ) a
  );
END;
$function$
;
