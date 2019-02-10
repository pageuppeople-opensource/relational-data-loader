IF object_id('SimpleTest') IS NOT NULL
	DROP TABLE SimpleTest

CREATE TABLE SimpleTest (
      Id INT PRIMARY KEY,
      StringCol VARCHAR(100),
)

INSERT [SimpleTest]
(
        Id,
        StringCol
)
SELECT	1,'Frank'
UNION ALL
SELECT	2,'Walker'
UNION ALL
SELECT	3,'National';
