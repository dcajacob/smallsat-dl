from smallsat_dl.titles import clean_title, sanitize_path_component


def test_clean_title_matches_notebook_cleanup():
    assert clean_title('"Dollar$ Sat"') == "Dollar Sat"


def test_sanitize_path_component_preserves_slash_marker_and_removes_unsafe_chars():
    assert sanitize_path_component('A/B: "Mission"$?') == "A-slash-B- Mission-"


def test_sanitize_path_component_has_fallback():
    assert sanitize_path_component(' "$" ') == "untitled"
