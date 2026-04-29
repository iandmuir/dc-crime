import re
from wswdy.ids import new_subscriber_id


def test_new_subscriber_id_shape():
    sid = new_subscriber_id()
    assert isinstance(sid, str)
    assert len(sid) == 8  # secrets.token_urlsafe(6) → 6 bytes → 8 base64url chars
    assert re.fullmatch(r"[A-Za-z0-9_-]+", sid)


def test_new_subscriber_id_unique():
    ids = {new_subscriber_id() for _ in range(2000)}
    assert len(ids) == 2000
