from wswdy.repos.fetch_log import last_attempt, last_successful, record_failure, record_success


def test_record_success_and_query(db):
    record_success(db, added=10, updated=2)
    last = last_successful(db)
    assert last["status"] == "ok"
    assert last["crimes_added"] == 10
    assert last["crimes_updated"] == 2


def test_record_failure(db):
    record_failure(db, error="boom")
    last = last_successful(db)
    assert last is None  # there is no successful fetch on record


def test_last_successful_returns_most_recent(db):
    record_success(db, added=1, updated=0)
    record_failure(db, error="x")
    record_success(db, added=5, updated=1)
    last = last_successful(db)
    assert last["crimes_added"] == 5


def test_last_attempt_returns_most_recent_regardless_of_status(db):
    record_success(db, added=1, updated=0)
    record_failure(db, error="network timeout")
    last = last_attempt(db)
    assert last["status"] == "failed"
    assert last["error"] == "network timeout"
