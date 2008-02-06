INSERT INTO ${dstTable} (
${dbFieldNames}
)
SELECT
${dbFieldNames}
FROM
    ${srcTable}
WHERE
    dbid <> 0
#if start
    AND dbid >= ${start}
#end
#if end
    AND dbid <= ${end}
#end