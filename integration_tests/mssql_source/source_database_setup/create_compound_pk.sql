IF object_id('CompoundPk') IS NULL
	CREATE TABLE CompoundPk (
      Id1 INT,
      Id2 INT ,
      CONSTRAINT  PK_CompoundPK PRIMARY  KEY (Id1, Id2))
ELSE
	TRUNCATE TABLE CompoundPk

INSERT CompoundPk
(
        Id1,
        Id2
)
SELECT	1,1
UNION ALL
SELECT	1,2
UNION ALL
SELECT	2,2
UNION ALL
SELECT	2,1



