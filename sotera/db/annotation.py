import six
import collections
import getpass


def validate_tag(pgsql_, tag):
    if isinstance(tag, six.string_types):
        with pgsql_, pgsql_.cursor() as cursor:
            cursor.execute(
                f"""SELECT EXISTS(
                      SELECT 1 FROM annotations.tag_set WHERE tag = '{tag}'
                    )"""
            )
            return cursor.fetchone()[0] if cursor.rowcount else False
    else:
        return False


def validate_feilds(return_fields, _valid_fields):
    rfields_ = []
    if isinstance(return_fields, six.string_types) and return_fields in _valid_fields:
        rfields_ = [return_fields]
    elif isinstance(return_fields, collections.abc.Iterable):
        for f in return_fields:
            if isinstance(f, six.string_types) and f in _valid_fields:
                rfields_.append(f)
            else:
                raise ValueError("Uknown field requested")
    else:
        raise ValueError("Uknown field requested")
    return rfields_


def insert_annotation(
    pgsql_,
    hid,
    block=None,
    start_time=None,
    stop_time=None,
    notes=None,
    tags=None,
    data=None,
):
    """Insert a new session annotation.

    Args:
        conn (object): database connection object (required)
        hid (int):  hid of session (required)
        block (int): block where annotation starts (optional)
        start_time (float): timestamp (utc unix time) of start of annotation (optional)
        stop_time (float): timestamp (utc unix time) of stop of annotation (optional)
        notes (text): free-text field for notes (optional)
        tags (string,list of strings): a signle tag (string), or list of tags to be
                                       attached to annoation (optional). Note that
                                       tags must already exist.
    Returns: annids
        int : annoation id for new annoation
           """

    annid = None

    s1 = """INSERT INTO annotations.annotations\n({})\nVALUES\n({})\nRETURNING id"""

    fields = []
    values = []
    if isinstance(hid, six.integer_types):
        fields.append("hid")
        values.append("{}".format(hid))
    else:
        raise ValueError("Provided hid has an undefiend value")

    if isinstance(block, six.integer_types):
        fields.append("block")
        values.append("{}".format(block))
    elif block is not None:
        raise ValueError("Provided block has an undefiend value")

    if isinstance(start_time, six.integer_types) or type(start_time) is float:
        fields.append("start_time")
        values.append("{}".format(start_time))
    elif start_time is not None:
        raise ValueError("Provided start_time has an undefiend value")

    if isinstance(stop_time, six.integer_types) or type(stop_time) is float:
        fields.append("stop_time")
        values.append("{}".format(stop_time))
    elif stop_time is not None:
        raise ValueError("Provided stop_time has an undefiend value")
    if isinstance(notes, six.string_types):
        fields.append("notes")
        values.append("'{}'".format(notes))
    elif notes is not None:
        raise ValueError("Provided notes has an undefiend value")

    if isinstance(data, six.string_types):
        fields.append("data")
        values.append("'{}'".format(data))
    elif data is not None:
        raise ValueError("Provided data has an undefiend value")

    tags_ = []
    if validate_tag(pgsql_, tags):
        tags_.append(tags)

    elif isinstance(tags, collections.abc.Iterable):
        for tag in tags:
            if validate_tag(pgsql_, tag):

                tags_.append(tag)
    if "source" not in str(tags):
        tags_.append("source:" + getpass.getuser())
    s2 = """INSERT into annotations.annotation_tags (tag,annid) VALUES ('{}',{})"""
    with pgsql_, pgsql_.cursor() as cursor:
        cursor.execute(s1.format(",".join(fields), ",".join(values)))
        annid = cursor.fetchone()[0]
        for tag in tags_:
            cursor.execute(s2.format(tag, annid))

    return annid


def update_annotation(
    pgsql_,
    annid,
    start_time=None,
    stop_time=None,
    notes=None,
    tags=None,
    data=None,
    deleted=None,
):
    """Update an existing session annotation.

    Args:
        conn (object): database connection object (required)
        annid (int): annoation id number (required)
        start_time (float): timestamp (utc unix time) of start of annotation (optional)
        stop_time (float): timestamp (utc unix time) of stop of annotation (optional)
        notes (text): free-text field for notes (optional)
        tags (string,list of strings): a signle tag (string), or list of tags to be
                                       attached to annoation (optional). Note that
                                       tags must already exist."""

    s1 = """ UPDATE annotations.annotations
             SET {params}
             WHERE
             id = {annid}
         """

    fields = []
    values = []

    if isinstance(start_time, six.integer_types) or type(start_time) is float:
        fields.append("start_time")
        values.append("{}".format(start_time))
    elif start_time is not None:
        raise ValueError("Provided start_time has an undefiend value")

    if isinstance(stop_time, six.integer_types) or type(stop_time) is float:
        fields.append("stop_time")
        values.append("{}".format(start_time))
    elif stop_time is not None:
        raise ValueError("Provided stop_time has an undefiend value")
    if isinstance(notes, six.string_types):
        fields.append("notes")
        values.append("'{}'".format(notes))
    elif notes is not None:
        raise ValueError("Provided notes has an undefiend value")

    if isinstance(data, six.string_types):
        fields.append("data")
        values.append("'{}'".format(data))
    elif data is not None:
        raise ValueError("Provided data has an undefiend value")

    if deleted:
        fields.append("deleted")
        values.append("'{}'".format(deleted))

    s1 = s1.format(
        annid=annid,
        params=", ".join("{}={}".format(f, z) for f, z in zip(fields, values)),
    )
    s2 = None
    s3 = None

    if tags is not None:
        tags_ = []
        if validate_tag(pgsql_, tags):
            tags_.append(tags)
        elif isinstance(tags, collections.abc.Iterable):
            for tag in tags:
                if validate_tag(pgsql_, tag):
                    tags_.append(tag)
        if len(tags_) > 0:
            s2 = """ INSERT INTO annotations.annotation_tags ("tag","annid")
                   VALUES {}
                   ON CONFLICT ("tag","annid")   DO NOTHING """.format(
                ",".join("('{}',{})".format(tag, annid) for tag in tags_)
            )
        s3 = """ DELETE FROM annotations.annotation_tags
               WHERE annid = {} AND tag NOT IN ({})
                """.format(
            annid, ",".join("'{}'".format(t) for t in tags_)
        )

    with pgsql_, pgsql_.cursor() as cursor:
        cursor.execute(s1)
        if s2 is not None:
            cursor.execute(s2)
        if s3 is not None:
            cursor.execute(s3)


def delete_annotation(pgsql_, annid):
    """Delete an  session annotation.

    Args:
        conn (object): database connection object (required)
        annid (int): annoation id number (required)
    """
    with pgsql_, pgsql_.cursor() as cursor:
        cursor.execute(
            f"""DELETE FROM annotations.annotation_tags WHERE annid={annid};
                DELETE FROM annotations.annotations WHERE id={annid}"""
        )


def find_annotations(
    pgsql_,
    hid=None,
    block=None,
    from_time=None,
    to_time=None,
    tags=None,
    return_fields=("id", "hid", "block", "start_time", "stop_time", "notes", "tags"),
):

    """Find annotations

    Args:
        conn (object): database connection object (required)
        hid (int):  hid of session (optional)
        block (int): block where annotation starts (optional)
        from_time (float): timestamp (utc unix time) of start of annotation (optional)
        to_time (float): timestamp (utc unix time) of stop of annotation (optional)
        tags (string,list of strings): a signle tag (string), or list of tags to be  to
            annoation (optional). Note that tags must already be exist.
    """
    joins = []
    wheres = []
    _valid_fields = (
        "id",
        "hid",
        "block",
        "start_time",
        "stop_time",
        "notes",
        "tags",
        "timestamp",
    )

    if from_time is not None or to_time is not None:
        raise ValueError("searching over times is not supported yet")

    if isinstance(hid, six.integer_types):
        wheres.append("ann.hid = {}".format(hid))
        if isinstance(block, six.integer_types):
            wheres.append("ann.block = {}".format(block))

    if isinstance(tags, six.string_types):
        joins.append("JOIN annotations.annotation_tags tags ON tags.annid = ann.id")
        wheres.append(f"tags.tag = '{tags}'")

    elif isinstance(tags, collections.abc.Iterable):
        for i, tag in enumerate(tags):
            if isinstance(tag, six.string_types):
                joins.append(
                    f"""JOIN annotations.annotation_tags tags{i}
                          ON tags{i}.annid = ann.id"""
                )
                wheres.append("tags{}.tag = '{}' ".format(i, tag))

    rfields_ = validate_feilds(return_fields, _valid_fields)

    if len(wheres) < 1:
        raise ValueError("Insufficient parameters supplied")

    fields = collections.OrderedDict()
    for f in rfields_:
        if f == "tags":
            fields[
                f
            ] = """(SELECT array_agg(tag)
                      FROM annotations.annotation_tags
                     WHERE annid = ann.id)"""
        else:
            fields[f] = "ann.{}".format(f)

    anns = []
    s = """SELECT {}
             FROM annotations.annotations ann {}
            WHERE {}
         ORDER BY ann.start_time"""
    with pgsql_, pgsql_.cursor() as cursor:
        cursor.execute(
            s.format(",".join(fields.values()), " ".join(joins), " AND ".join(wheres))
        )
        for row in cursor:
            a = {}
            for i, key in enumerate(fields):
                a[key] = row[i]
            anns.append(a.copy())
    return anns
