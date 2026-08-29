from src.resolver.entity_resolver import EntityResolver, normalize


def _resolver():
    return EntityResolver.from_seed_file("config/entity_seed.yaml")


def test_normalize_strips_legal_suffix():
    assert normalize("OpenAI, Inc.") == "openai"
    assert normalize("Scale AI, Inc.") == "scale"


def test_exact_and_alias_and_fuzzy():
    r = _resolver()
    assert r.resolve_str("OpenAI") == "OpenAI"
    assert r.resolve_str("Open AI") == "OpenAI"
    assert r.resolve_str("OpenAI, Inc.") == "OpenAI"
    assert r.resolve_str("Hugging  Face") == "Hugging Face"
    assert r.resolve_str("huggingface") == "Hugging Face"


def test_new_entity_is_minted_and_reused():
    r = _resolver()
    a = r.resolve("Quokka Labs")
    assert a.is_new_entity and a.method == "new"
    b = r.resolve("quokka labs")
    assert b.canonical == a.canonical and not b.is_new_entity


def test_mapping_log_records_every_call():
    r = _resolver()
    r.resolve_str("OpenAI")
    r.resolve_str("Cohere AI")
    assert len(r.mapping_log) == 2
    assert {m.method for m in r.mapping_log} <= {"exact", "alias", "fuzzy", "new"}
