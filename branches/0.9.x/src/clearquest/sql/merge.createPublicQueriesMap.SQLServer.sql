IF EXISTS
   (SELECT 1
    FROM   %(dboTablePrefix)s.sysobjects
    WHERE  name = '%(shortName)s' AND type = 'U')
        DROP TABLE %(publicQueriesMapTableName)s;

CREATE TABLE %(publicQueriesMapTableName)s (
    dbid INT,
    parent_dbid INT,
    type INT,
    subtype INT,
    ratl_mastership INT,
    name NVARCHAR(250),
    path NVARCHAR(%(publicQueriesMapMaxPathLength)d),
)

CREATE UNIQUE CLUSTERED INDEX dbid_cix ON %(publicQueriesMapTableName)s (dbid)
WITH (
    FILLFACTOR = 20,
    SORT_IN_TEMPDB = ON,
    ALLOW_ROW_LOCKS = OFF,
    ALLOW_PAGE_LOCKS = OFF
)

