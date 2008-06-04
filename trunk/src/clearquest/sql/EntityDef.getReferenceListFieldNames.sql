SELECT
    f.name
FROM
    fielddef f
INNER JOIN
    entitydef e ON
        e.id = f.entitydef_id
WHERE
    f.ref_role = 1 AND
    e.name = '${this.GetName()}'
