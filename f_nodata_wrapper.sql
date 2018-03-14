CREATE OR REPLACE FUNCTION nodata_wrapper(IN section_text text) RETURNS text SET search_path=@extschema@,public AS $$
BEGIN
    IF section_text IS NULL OR section_text = '' THEN
        RETURN '<p>No data in this section</p>';
    ELSE
        RETURN section_text;
    END IF;
END;
$$ LANGUAGE plpgsql;
