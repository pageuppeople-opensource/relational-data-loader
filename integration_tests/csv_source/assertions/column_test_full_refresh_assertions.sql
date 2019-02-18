SET client_encoding TO 'UTF8';

DROP TABLE IF EXISTS results;

CREATE TEMPORARY TABLE results AS
WITH
    expected(id, int_column_1, date_column_1, decimal_column_1, date_time_column_1, string_column_1, guid_column_1, big_int_column_1, bool_column_1)
    AS
    (
        SELECT
        1, 111.0, '1976-12-01'::DATE,
        12.1212, '1976-12-01 01:00:00.000000'::TIMESTAMP,
        'A Basic String', '57bc8093-fe4c-477a-bbd7-fb5c02055a7e'::UUID,2147483647121212,
        True
    UNION ALL
        SELECT 2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
    UNION ALL
        SELECT 3, 333.0, '2001-01-01', 33.333, NULL, 'This Text Has a Quote Before "Dave', NULL, NULL, True
    UNION ALL
        SELECT 4, NULL, NULL, NULL, NULL, 'ം ഃ അ ആ ഇ ഈ ഉ ഊ ഋ ഌ എ ഏ', 'aabc8093-fe4c-477a-bbd7-fb5c02055a7e', NULL, False
    UNION ALL
        SELECT 5, NULL, NULL, NULL, NULL, 'This row will be updated in the incremental review test', NULL, NULL, NULL
    ),

    actual
    AS
    (
    SELECT id, int_column_1, date_column_1, decimal_column_1, date_time_column_1, string_column_1, guid_column_1, big_int_column_1, bool_column_1
    FROM rdl_integration_tests.load_source_data
    )

    SELECT *
    FROM expected
    EXCEPT
    SELECT *
    FROM actual;

DO $$
BEGIN
    PERFORM * FROM results;
IF FOUND THEN RAISE EXCEPTION '[FULL REFRESH TEST] FAIL: The actual data did not match the expected data for the CSV refresh';
    ELSE
    RAISE NOTICE '[FULL REFRESH TEST] PASS';
END
IF;
END $$;
