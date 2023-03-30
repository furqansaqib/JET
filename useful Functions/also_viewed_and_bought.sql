CREATE OR REPLACE FUNCTION jet.find_matching_ids(string1 text, string2 text)
 RETURNS text
 LANGUAGE plpgsql
AS $function$
DECLARE
  arr1 text[];
  arr2 text[];
BEGIN
  string1 := regexp_replace(string1, '[{}"]', '', 'g');
  string2 := regexp_replace(string2, '[{}"]', '', 'g');
  arr1 := string_to_array(string1, ',');
  arr2 := string_to_array(string2, ',');
  RETURN format('[%s]', array_to_string(ARRAY(
    SELECT unnest(arr1) INTERSECT SELECT unnest(arr2)
  ), ','));
END;
$function$
;
