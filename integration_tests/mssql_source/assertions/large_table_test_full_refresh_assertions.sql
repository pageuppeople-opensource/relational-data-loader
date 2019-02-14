SET client_encoding TO 'UTF8';

DO $$
  BEGIN
  IF (SELECT COUNT(*) FROM rdl_integration_tests.load_large_data ) = 1000000 THEN
    RAISE NOTICE '[LARGE MSSQL IMPORT TEST] PASS';
  ELSE
    RAISE EXCEPTION '[LARGE MSSQL IMPORT TEST] FAIL: Did not find the required 1,000,000 rows.';
  END IF;
END $$;
