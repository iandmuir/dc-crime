import re
from wswdy.ids import new_subscriber_id


def test_new_subscriber_id_shape():
    sid = new_subscriber_id()
    assert isinstance(sid, str)
    assert 6 <= len(sid) <= 16
    assert re.fullmatch(r"[A-Za-z0-9_-]+", sid)


def test_new_subscriber_id_unique():
    ids = {new_subscriber_id() for _ in range(2000)}
    assert len(ids) == 2000
