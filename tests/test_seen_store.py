from src.crawler.seen_store import SqliteSeenStore, normalize_url


def test_normalize_url_strips_tracking_and_trailing_slash():
    a = normalize_url("https://Example.com/Post/?utm_source=x&id=5#frag")
    b = normalize_url("https://example.com/Post?id=5")
    assert a == b


def test_add_if_new(tmp_path):
    store = SqliteSeenStore(tmp_path / "seen.sqlite")
    url = "https://example.com/a?b=1"
    assert store.add_if_new(url) is True
    assert store.add_if_new(url) is False
    assert store.add_if_new("https://example.com/a?b=1&utm_campaign=q") is False
    assert store.seen(url)
    store.close()
