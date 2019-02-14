WITH
        Numbers (n)
        AS
        (
                SELECT 999999999999999
        )

INSERT LargeTable
        (
        Id,
        StringColumn1,
        DateColumn1,
        DateColumn2,
        IntColumn1,
        StringColumn2,
        GuidColumn,
        BoolColumn
        )
SELECT n,
        CASE WHEN n % 3 = 0 THEN NULL ELSE 'Row Number ' + CAST(n as varchar) END,
        CASE WHEN n % 5 = 0 THEN NULL ELSE DateAdd(hour, -n, '2000-01-1') END,
        CASE WHEN n % 7 = 0 THEN NULL ELSE DateAdd(hour, n, '2000-01-1') END,
        CASE WHEN n % 9 = 0 THEN NULL ELSE n * 1000 END,
        CASE WHEN n % 11 = 0 THEN NULL ELSE N'काचं शक्नोम्यत्तुम् । नोपहिनस्ति माम् ॥' END,
        CASE WHEN n % 13 = 0 THEN NULL ELSE newid() END,
        CASE WHEN n % 3 = 0 THEN NULL ELSE 1 END

FROM Numbers
