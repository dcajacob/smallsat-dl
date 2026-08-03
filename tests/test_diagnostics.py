import json

from smallsat_dl.diagnostics import JsonlDiagnostics


def test_jsonl_diagnostics_flushes_structured_events(tmp_path):
    path = tmp_path / "diagnostics.jsonl"
    diagnostics = JsonlDiagnostics(path)

    diagnostics.record("download_start", url="https://example.test/paper.pdf")
    diagnostics.close()

    payload = json.loads(path.read_text())
    assert payload["event"] == "download_start"
    assert payload["url"] == "https://example.test/paper.pdf"
    assert payload["elapsed_seconds"] >= 0
    assert payload["timestamp"].endswith("+00:00")
