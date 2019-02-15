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
SELECT 2000000,
        'test change',
        '1999-01-1',
        '1999-02-1',
        -2,
        'foobar',
        newid(),
        1
