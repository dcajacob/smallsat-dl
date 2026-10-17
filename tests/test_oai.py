import asyncio
import json
from urllib.parse import parse_qs, urlsplit
from xml.sax.saxutils import escape

import pytest

from smallsat_dl.oai import (
    OaiProtocolError,
    OaiRecord,
    OaiUnavailable,
    discover_records,
    endpoint_for_base_url,
    parse_list_records,
    records_from_json,
    records_to_json,
)


ENDPOINT = "https://archive.test/do/oai/"
EVENT_URL = "https://digitalcommons.usu.edu/smallsat/2025/all2025/6"
PRIMARY_URL = "https://digitalcommons.usu.edu/context/smallsat/article/6246/viewcontent/Paper.pdf"
SUPPLEMENTAL_URL = (
    "https://digitalcommons.usu.edu/context/smallsat/article/6246/filename/0/"
    "type/additional/viewcontent/Slides.pptx"
)
NS = 'xmlns="http://www.openarchives.org/OAI/2.0/"'


def record_xml(identifier="smallsat-6246", *, event_url=EVENT_URL, primary_url=PRIMARY_URL,
               supplemental_urls=(SUPPLEMENTAL_URL,), title="Mission & Science", deleted=False):
    status = ' status="deleted"' if deleted else ""
    header = (
        f'<header{status}>'
        f'<identifier>oai:archive.test:{identifier}</identifier>'
        '<datestamp>2026-10-01T00:00:00Z</datestamp></header>'
    )
    if deleted:
        return f"<record>{header}</record>"
    urls = [event_url, "info:doi/10.26077/example", "https://doi.org/10.26077/example"]
    if primary_url is not None:
        urls.append(primary_url)
    urls.extend(supplemental_urls)
    identifiers = "".join(f"<dc:identifier>{escape(url)}</dc:identifier>" for url in urls)
    return (
        f'<record>{header}<metadata><oai_dc:dc '
        'xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/" '
        'xmlns:dc="http://purl.org/dc/elements/1.1/">'
        f'<dc:title>{escape(title)}</dc:title>'
        '<dc:date>2026-08-24T00:00:00Z</dc:date>'
        '<dc:format>application/pdf</dc:format>'
        f'{identifiers}</oai_dc:dc></metadata></record>'
    )


def response_xml(*records, token=None, complete_size=None):
    size = f' completeListSize="{complete_size}"' if complete_size is not None else ""
    resumption = (
        f"<resumptionToken{size}>{escape(token)}</resumptionToken>"
        if token is not None else ""
    )
    return f'<OAI-PMH {NS}><ListRecords>{"".join(records)}{resumption}</ListRecords></OAI-PMH>'


def error_xml(code, message="Protocol error"):
    return f'<OAI-PMH {NS}><error code="{code}">{escape(message)}</error></OAI-PMH>'


def test_native_urls_keep_filenames_and_doi_does_not_replace_primary():
    page = parse_list_records(response_xml(record_xml(), token="next/token", complete_size=288))

    assert page.records == (OaiRecord(
        identifier="oai:archive.test:smallsat-6246", title="Mission & Science",
        event_url=EVENT_URL, year=2025, primary_url=PRIMARY_URL,
        supplemental_urls=(SUPPLEMENTAL_URL,), formats=("application/pdf",),
    ),)
    assert page.resumption_token == "next/token"
    assert page.complete_list_size == 288


def test_query_style_supplement_and_external_primary_are_supported():
    supplemental = "https://archive.test/cgi/viewcontent.cgi?type=additional&filename=0&article=1"
    primary = "https://publisher.test/download/paper.pdf"

    page = parse_list_records(response_xml(record_xml(
        primary_url=primary, supplemental_urls=(supplemental, supplemental),
    )))

    assert page.records[0].primary_url == primary
    assert page.records[0].supplemental_urls == (supplemental,)


def test_supplement_only_record_has_no_primary():
    page = parse_list_records(response_xml(record_xml(primary_url=None)))

    assert page.records[0].primary_url is None
    assert page.records[0].supplemental_urls == (SUPPLEMENTAL_URL,)


def test_missing_files_are_valid_metadata_and_non_year_records_have_no_year():
    page = parse_list_records(response_xml(record_xml(
        event_url="https://digitalcommons.usu.edu/smallsat/lifetimeachievement/1",
        primary_url=None, supplemental_urls=(),
    )))

    assert page.records[0].year is None
    assert page.records[0].primary_url is None
    assert page.records[0].supplemental_urls == ()


def test_deleted_records_are_ignored_without_requiring_metadata():
    page = parse_list_records(response_xml(record_xml(deleted=True)))

    assert page.records == ()
    assert not page.no_records_match


def test_no_records_match_is_distinct_from_protocol_failure():
    page = parse_list_records(error_xml("noRecordsMatch"))

    assert page.no_records_match
    assert page.records == ()
    assert page.resumption_token is None


@pytest.mark.parametrize("code", ["badArgument", "badResumptionToken", "cannotDisseminateFormat"])
def test_oai_error_codes_are_preserved(code):
    with pytest.raises(OaiProtocolError) as error:
        parse_list_records(error_xml(code, "The request cannot be processed"))

    assert error.value.code == code
    assert error.value.message == "The request cannot be processed"


@pytest.mark.parametrize("xml", [
    '<html><title>Verify you are human</title></html>',
    '<OAI-PMH>',
    '<OAI-PMH><ListRecords /></OAI-PMH>',
    f'<OAI-PMH {NS}><Identify /></OAI-PMH>',
    response_xml(),
    response_xml('<record><metadata /></record>'),
    response_xml('<record><header><identifier>missing-metadata</identifier></header></record>'),
    response_xml(record_xml(title="")),
    response_xml(record_xml(event_url="info:doi/10.26077/example")),
    response_xml(record_xml(event_url="http://[")),
    response_xml(record_xml(), token="next", complete_size="unknown"),
    response_xml(record_xml(), token="next", complete_size=-1),
])
def test_unrecognized_or_malformed_responses_are_not_empty_successes(xml):
    with pytest.raises(OaiProtocolError):
        parse_list_records(xml)


def test_pagination_uses_opaque_token_without_initial_set_or_prefix():
    token = "next/token?x=1&literal=+ space"
    pages = [
        response_xml(record_xml(), token=token, complete_size=2),
        response_xml(record_xml("smallsat-2", event_url=EVENT_URL + "2"), token=""),
    ]
    urls = []

    async def get_text(url):
        urls.append(url)
        return pages.pop(0)

    records = asyncio.run(discover_records(
        get_text, endpoint=ENDPOINT, set_spec="publication:smallsat/2025",
    ))

    assert len(records) == 2
    assert parse_qs(urlsplit(urls[0]).query) == {
        "verb": ["ListRecords"], "metadataPrefix": ["oai_dc"],
        "set": ["publication:smallsat/2025"],
    }
    assert parse_qs(urlsplit(urls[1]).query) == {
        "verb": ["ListRecords"], "resumptionToken": [token],
    }


def test_unchanged_duplicate_records_do_not_duplicate_results():
    pages = [response_xml(record_xml(), token="next"), response_xml(record_xml())]

    async def get_text(url):
        return pages.pop(0)

    assert len(asyncio.run(discover_records(get_text))) == 1


@pytest.mark.parametrize("second_page", [
    response_xml(record_xml(), token="same"),
    response_xml(record_xml(title="Changed title")),
    error_xml("noRecordsMatch"),
])
def test_invalid_continuation_fails_instead_of_returning_partial_records(second_page):
    pages = [response_xml(record_xml(), token="same"), second_page]

    async def get_text(url):
        return pages.pop(0)

    with pytest.raises(OaiProtocolError):
        asyncio.run(discover_records(get_text))


def test_unique_unending_tokens_hit_page_limit():
    calls = []

    async def get_text(url):
        calls.append(url)
        return response_xml(record_xml(str(len(calls))), token=f"page-{len(calls)}")

    with pytest.raises(OaiProtocolError) as error:
        asyncio.run(discover_records(get_text, max_pages=2))

    assert error.value.code == "pageLimit"
    assert len(calls) == 2


def test_premature_end_cannot_return_a_partial_advertised_harvest():
    async def get_text(url):
        return response_xml(record_xml(), token="", complete_size=288)

    with pytest.raises(OaiProtocolError, match="received 1 of 288"):
        asyncio.run(discover_records(get_text))


def test_complete_list_size_includes_deleted_records():
    async def get_text(url):
        return response_xml(
            record_xml(), record_xml("deleted", deleted=True), token="", complete_size=2,
        )

    assert len(asyncio.run(discover_records(get_text))) == 1


def test_advertised_list_size_cannot_change_during_pagination():
    pages = [
        response_xml(record_xml(), token="next", complete_size=2),
        response_xml(record_xml("second"), token="", complete_size=3),
    ]

    async def get_text(url):
        return pages.pop(0)

    with pytest.raises(OaiProtocolError, match="completeListSize changed"):
        asyncio.run(discover_records(get_text))


def test_unavailable_endpoint_does_not_look_like_no_records():
    async def get_text(url):
        raise TimeoutError("metadata request timed out")

    with pytest.raises(OaiUnavailable, match="timed out"):
        asyncio.run(discover_records(get_text))


def test_initial_no_records_match_returns_an_empty_complete_harvest():
    async def get_text(url):
        return error_xml("noRecordsMatch")

    assert asyncio.run(discover_records(get_text)) == []


@pytest.mark.parametrize("base_url,expected", [
    ("https://digitalcommons.usu.edu/smallsat/", "https://digitalcommons.usu.edu/do/oai/"),
    ("http://127.0.0.1:8080/smallsat/", "http://127.0.0.1:8080/do/oai/"),
])
def test_endpoint_derivation_preserves_repository_origin(base_url, expected):
    assert endpoint_for_base_url(base_url) == expected


def test_complete_harvest_cache_round_trips_without_tokens():
    records = list(parse_list_records(response_xml(record_xml())).records)

    cached = records_to_json(records)

    assert records_from_json(cached) == records
    assert "resumption" not in cached
    assert records_from_json(records_to_json([])) == []


@pytest.mark.parametrize("value", [
    "not json", "null", "[]", '{"version": 1}',
    '{"version": true, "records": []}', '{"version": 2, "records": []}',
    '{"version": 1, "records": [null]}',
])
def test_invalid_cache_snapshots_are_rejected(value):
    with pytest.raises(OaiProtocolError) as error:
        records_from_json(value)

    assert error.value.code == "invalidCache"


@pytest.mark.parametrize("key,value", [
    ("year", 2026), ("year", "2025"), ("year", True),
    ("event_url", "/relative/url"), ("primary_url", "ftp://archive.test/file"),
    ("supplemental_urls", ["/relative/url"]), ("formats", "application/pdf"),
])
def test_invalid_cached_record_fields_are_rejected(key, value):
    records = list(parse_list_records(response_xml(record_xml())).records)
    snapshot = json.loads(records_to_json(records))
    snapshot["records"][0][key] = value

    with pytest.raises(OaiProtocolError) as error:
        records_from_json(json.dumps(snapshot))

    assert error.value.code == "invalidCache"
