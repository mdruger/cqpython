UPDATE
    ${dstPrefix}.attachments_blob
SET
    attachments_dbid = attachments_dbid + ${attachmentsDbIdOffset},
    entity_dbid = entity_dbid + ${dbidOffset},
    ratl_mastership = NULL
WHERE
    ratl_mastership = ${sourceReplicaId} AND
    EXISTS
       (SELECT
            1
        FROM
            ${dstPrefix}.attachments a
        WHERE
            a.dbid = attachments_dbid + ${attachmentsDbIdOffset})
GO
DELETE
    ${dstPrefix}.attachments_blob
WHERE
    ratl_mastership = ${sourceReplicaId}
            