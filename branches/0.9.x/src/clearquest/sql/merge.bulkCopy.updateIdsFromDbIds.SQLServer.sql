UPDATE
    ${dstTable}
SET
    id =
       (dbg.site_name + 
        REPLICATE(0, 8-LEN(CAST((dbid - 0x2000000) AS CHAR))) +
        CAST((dbid - 0x2000000) AS CHAR))
FROM
    ${dstPrefix}.dbglobal dbg
WHERE
    dbid <> 0 AND
    ratl_mastership = ${sourceReplicaId}