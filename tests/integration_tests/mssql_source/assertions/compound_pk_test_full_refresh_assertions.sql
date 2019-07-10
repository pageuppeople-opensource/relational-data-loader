SET client_encoding
TO 'UTF8';

DO $$
BEGIN
  IF ((SELECT COUNT(*)
    FROM rdl_integration_tests.load_compound_pk ) = 4)
    OR
    ((SELECT COUNT(*)
    FROM rdl_integration_tests.load_compound_pk ) = 10) THEN
    RAISE NOTICE '[COMPOUND KEY MSSQL IMPORT TEST] PASS';
ELSE
    RAISE EXCEPTION '[COMPOUND KEY MSSQL IMPORT TEST] FAIL: Did not find the required 4 (or 10) rows.';
END
IF;
END $$;
