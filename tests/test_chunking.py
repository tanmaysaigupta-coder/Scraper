from src.llm.chunking import ChunkPlan, chunk_document, estimate_tokens, pick_primary_chunk

PLAN = ChunkPlan(chars_per_token=4, safety_ratio=0.8, overlap_chars=50)


def test_short_doc_single_chunk():
    doc = "one paragraph, small.\n\nsecond one."
    assert chunk_document(doc, 100_000, PLAN) == [doc]


def test_large_doc_is_split_under_budget():
    para = ("Sentence number {} with enough words to matter here. ".format)
    doc = "\n\n".join(para(i) * 40 for i in range(200))
    chunks = chunk_document(doc, 2_000, PLAN)  # tiny budget forces splitting
    assert len(chunks) > 1
    budget = PLAN.budget_chars(2_000)
    assert all(len(c) <= budget + PLAN.overlap_chars + 10 for c in chunks)


def test_shrink_halves_safety_ratio():
    assert PLAN.shrink().safety_ratio == PLAN.safety_ratio * 0.5


def test_pick_primary_prefers_signal_dense_chunk():
    dull = "the weather was mild and the road was long " * 50
    rich = "pricing FREEMIUM employees founded github stars arxiv authors published " * 10
    assert pick_primary_chunk([dull, rich]) == rich


def test_estimate_tokens():
    assert estimate_tokens("x" * 400, 4) == 100
