IF object_id('LargeTable') IS NULL
	CREATE TABLE LargeTable (
      Id INT PRIMARY KEY,
      StringColumn1 VARCHAR(100),
      DateColumn1	DATETIME,
      DateColumn2	DATE,
      IntColumn1 INT,
      StringColumn2 NVARCHAR(100),
      GuidColumn UNIQUEIDENTIFIER)
ELSE
	TRUNCATE TABLE LargeTable

;WITH Numbers (n) AS
(
    SELECT ROW_NUMBER() OVER (ORDER BY (SELECT NULL))
    FROM (VALUES(0),(0),(0),(0),(0),(0),(0),(0),(0),(0)) a(n)
    CROSS JOIN (VALUES(0),(0),(0),(0),(0),(0),(0),(0),(0),(0)) b(n)
    CROSS JOIN (VALUES(0),(0),(0),(0),(0),(0),(0),(0),(0),(0)) c(n)
	  CROSS JOIN (VALUES(0),(0),(0),(0),(0),(0),(0),(0),(0),(0)) d(n)
	  CROSS JOIN (VALUES(0),(0),(0),(0),(0),(0),(0),(0),(0),(0)) e(n)
	  CROSS JOIN (VALUES(0),(0),(0),(0),(0),(0),(0),(0),(0),(0)) f(n)
)

INSERT LargeTable
(
        Id,
        StringColumn1,
        DateColumn1,
        DateColumn2,
        IntColumn1,
        StringColumn2,
        GuidColumn
)
SELECT	n,
        CASE WHEN n % 3 = 0 THEN NULL ELSE 'Row Number ' + CAST(n as varchar) END,
        CASE WHEN n % 5 = 0 THEN NULL ELSE DateAdd(hour, -n, '2000-01-1') END,
        CASE WHEN n % 7 = 0 THEN NULL ELSE DateAdd(hour, n, '2000-01-1') END,
        CASE WHEN n % 9 = 0 THEN NULL ELSE n * 1000 END,
        CASE WHEN n % 11 = 0 THEN NULL ELSE N'काचं शक्नोम्यत्तुम् । नोपहिनस्ति माम् ॥' END,
        CASE WHEN n % 13 = 0 THEN NULL ELSE newid() END
FROM    Numbers



