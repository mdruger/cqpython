UPDATE
    ${dstPrefix}.attachments
SET
    entity_dbid = entity_dbid + ${dbidOffset},
    ratl_mastership = NULL
WHERE
    ratl_mastership = ${sourceReplicaId} AND
    EXISTS
       (SELECT
            1
        FROM
            ${dstTable} dst
        WHERE
            dst.dbid = entity_dbid + ${dbidOffset} AND
            dst.ratl_mastership = ${sourceReplicaId})
