#if addReplicaColumn
IF NOT EXISTS
       (SELECT
            1
        FROM
            INFORMATION_SCHEMA.COLUMNS
        WHERE
            {fn LCASE(TABLE_NAME)} = {fn LCASE('${tableName}')} AND
            {fn LCASE(COLUMN_NAME)} = {fn LCASE('ratl_mastership')})
    ALTER TABLE ${dstTable} ADD ratl_mastership INT
GO
#end
INSERT INTO ${dstTable} (
    ${dstColumns}
)
SELECT
    ${srcColumns}
FROM
    ${srcTable} src
${where}
${orderBy}