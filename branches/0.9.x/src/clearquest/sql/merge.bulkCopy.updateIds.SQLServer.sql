UPDATE
    ${dstPrefix}.${dstTable}
SET
    id = dbg.site_name + RIGHT(id, 8)
FROM
    ${dstPrefix}.dbglobal dbg
WHERE
    dbid <> 0 AND
    LEFT(id, LEN(dbg.site_name)) <> dbg.site_name AND
    ratl_mastership = ${replicaId}