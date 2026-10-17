"""Digital Commons OAI-PMH discovery, independent of HTML and download code."""

from __future__ import annotations

import json
import re
import xml.etree.ElementTree as ET
from collections.abc import Awaitable, Callable
from dataclasses import asdict, dataclass
from urllib.parse import parse_qs, urlencode, urlsplit, urlunsplit


DEFAULT_ENDPOINT = "https://digitalcommons.usu.edu/do/oai/"
OAI_NS = "http://www.openarchives.org/OAI/2.0/"
DC_NS = "http://purl.org/dc/elements/1.1/"
OAI_DC_NS = "http://www.openarchives.org/OAI/2.0/oai_dc/"
EVENT_YEAR_RE = re.compile(r"/smallsat/(\d{4})/")
GetText = Callable[[str], Awaitable[str]]


class OaiError(RuntimeError):
    """Base class for failed OAI discovery; never represents an empty result."""


class OaiUnavailable(OaiError):
    """The metadata endpoint could not be fetched successfully."""


class OaiProtocolError(OaiError):
    """An OAI error response or invalid/incomplete protocol response."""

    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"OAI-PMH {code}: {message}")


@dataclass(frozen=True)
class OaiRecord:
    identifier: str
    title: str
    event_url: str
    year: int | None
    primary_url: str | None
    supplemental_urls: tuple[str, ...] = ()
    formats: tuple[str, ...] = ()


@dataclass(frozen=True)
class OaiPage:
    records: tuple[OaiRecord, ...]
    resumption_token: str | None = None
    complete_list_size: int | None = None
    no_records_match: bool = False
    record_count: int = 0


def endpoint_for_base_url(base_url: str) -> str:
    """Use the repository origin, preserving a custom HTTP(S) test server."""
    parsed = urlsplit(base_url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError("OAI discovery requires an absolute HTTP(S) base URL")
    return urlunsplit((parsed.scheme, parsed.netloc, "/do/oai/", "", ""))


def _text(element: ET.Element | None) -> str:
    return " ".join("".join(element.itertext()).split()) if element is not None else ""


def _http_url(value: str) -> bool:
    try:
        parsed = urlsplit(value)
        return parsed.scheme in {"http", "https"} and bool(parsed.netloc)
    except ValueError:
        return False


def _is_doi(value: str) -> bool:
    return (urlsplit(value).hostname or "").casefold() in {"doi.org", "dx.doi.org"}


def _is_supplemental(value: str) -> bool:
    parsed = urlsplit(value)
    return "/type/additional/" in parsed.path or "additional" in parse_qs(parsed.query).get(
        "type", []
    )


def _parse_record(record: ET.Element) -> OaiRecord | None:
    header = record.find(f"{{{OAI_NS}}}header")
    if header is None:
        raise OaiProtocolError("invalidRecord", "record has no header")
    if header.get("status") == "deleted":
        return None
    identifier = _text(header.find(f"{{{OAI_NS}}}identifier"))
    metadata = record.find(f"{{{OAI_NS}}}metadata/{{{OAI_DC_NS}}}dc")
    if not identifier or metadata is None:
        raise OaiProtocolError("invalidRecord", "record has no identifier or oai_dc metadata")
    title = _text(metadata.find(f"{{{DC_NS}}}title"))
    identifiers = [_text(element) for element in metadata.findall(f"{{{DC_NS}}}identifier")]
    # Digital Commons exposes the article landing page first. DOI identifiers may
    # appear before the primary file, so later elements must be classified by URL.
    if not title or not identifiers or not _http_url(identifiers[0]) or _is_doi(identifiers[0]):
        raise OaiProtocolError("invalidRecord", f"record {identifier} has no title or landing URL")
    event_url = identifiers[0]
    files = list(dict.fromkeys(
        value for value in identifiers[1:]
        if _http_url(value) and not _is_doi(value) and value != event_url
    ))
    primary = [value for value in files if not _is_supplemental(value)]
    if len(primary) > 1:
        raise OaiProtocolError("invalidRecord", f"record {identifier} has ambiguous primary files")
    year_match = EVENT_YEAR_RE.search(urlsplit(event_url).path)
    formats = tuple(dict.fromkeys(
        value for element in metadata.findall(f"{{{DC_NS}}}format")
        if (value := _text(element))
    ))
    return OaiRecord(
        identifier=identifier,
        title=title,
        event_url=event_url,
        year=int(year_match.group(1)) if year_match is not None else None,
        primary_url=primary[0] if primary else None,
        supplemental_urls=tuple(value for value in files if _is_supplemental(value)),
        formats=formats,
    )


def parse_list_records(xml: str) -> OaiPage:
    """Parse one oai_dc ListRecords response, accepting noRecordsMatch as empty."""
    try:
        root = ET.fromstring(xml)
    except (ET.ParseError, TypeError) as exc:
        raise OaiProtocolError("invalidXML", "metadata response is not valid XML") from exc
    if root.tag != f"{{{OAI_NS}}}OAI-PMH":
        raise OaiProtocolError("invalidResponse", "metadata response is not OAI-PMH 2.0")
    errors = root.findall(f"{{{OAI_NS}}}error")
    listing = root.find(f"{{{OAI_NS}}}ListRecords")
    if errors:
        if len(errors) == 1 and errors[0].get("code") == "noRecordsMatch" and listing is None:
            return OaiPage((), no_records_match=True)
        error = next((item for item in errors if item.get("code") != "noRecordsMatch"), errors[0])
        raise OaiProtocolError(error.get("code", "invalidResponse"), _text(error))
    if listing is None:
        raise OaiProtocolError("invalidResponse", "metadata response has no ListRecords element")
    raw_records = listing.findall(f"{{{OAI_NS}}}record")
    if not raw_records:
        raise OaiProtocolError("invalidResponse", "empty ListRecords without noRecordsMatch")
    records = tuple(parsed for record in raw_records if (parsed := _parse_record(record)) is not None)
    token_element = listing.find(f"{{{OAI_NS}}}resumptionToken")
    token = ("".join(token_element.itertext()).strip() or None) if token_element is not None else None
    size = token_element.get("completeListSize") if token_element is not None else None
    try:
        complete_size = int(size) if size is not None else None
        if complete_size is not None and complete_size < 0:
            raise ValueError
    except ValueError as exc:
        raise OaiProtocolError("invalidResponse", "invalid completeListSize") from exc
    return OaiPage(records, token, complete_size, record_count=len(raw_records))


async def discover_records(
    get_text: GetText,
    *,
    endpoint: str = DEFAULT_ENDPOINT,
    set_spec: str = "publication:smallsat",
    max_pages: int = 1000,
) -> list[OaiRecord]:
    """Harvest a complete set or fail, without retaining tokens between runs.

    Use ``publication:smallsat/YEAR`` for a conference year. OAI ``from`` and
    ``until`` filter record modification dates rather than conference dates.
    """
    if max_pages < 1:
        raise ValueError("max_pages must be at least 1")
    parsed_endpoint = urlsplit(endpoint)
    if not _http_url(endpoint) or parsed_endpoint.query or parsed_endpoint.fragment:
        raise ValueError("OAI endpoint must be an absolute HTTP(S) URL without a query or fragment")
    if not set_spec:
        raise ValueError("OAI set must not be empty")
    parameters = {"verb": "ListRecords", "metadataPrefix": "oai_dc", "set": set_spec}
    records_by_id: dict[str, OaiRecord] = {}
    seen_tokens: set[str] = set()
    expected_size: int | None = None
    record_count = 0
    for page_index in range(max_pages):
        url = f"{endpoint}?{urlencode(parameters)}"
        try:
            xml = await get_text(url)
        except Exception as exc:
            raise OaiUnavailable(f"Could not fetch OAI metadata: {exc}") from exc
        page = parse_list_records(xml)
        if page.no_records_match:
            if page_index:
                raise OaiProtocolError("invalidResponse", "noRecordsMatch during pagination")
            return []
        if page.complete_list_size is not None:
            if expected_size is not None and expected_size != page.complete_list_size:
                raise OaiProtocolError("incompleteList", "completeListSize changed during pagination")
            expected_size = page.complete_list_size
        record_count += page.record_count
        for record in page.records:
            previous = records_by_id.get(record.identifier)
            if previous is not None and previous != record:
                raise OaiProtocolError("invalidRecord", "record changed during pagination")
            records_by_id[record.identifier] = record
        if page.resumption_token is None:
            if expected_size is not None and record_count != expected_size:
                raise OaiProtocolError(
                    "incompleteList", f"received {record_count} of {expected_size} advertised records",
                )
            return list(records_by_id.values())
        if page.resumption_token in seen_tokens:
            raise OaiProtocolError("paginationLoop", "server repeated a resumption token")
        seen_tokens.add(page.resumption_token)
        parameters = {"verb": "ListRecords", "resumptionToken": page.resumption_token}
    raise OaiProtocolError("pageLimit", f"metadata harvest exceeded {max_pages} pages")


def records_to_json(records: list[OaiRecord]) -> str:
    """Serialize a completed harvest, never its intermediate resumption tokens."""
    return json.dumps({"version": 1, "records": [asdict(record) for record in records]})


def records_from_json(value: str) -> list[OaiRecord]:
    """Read a versioned cache snapshot, rejecting incomplete or invalid shapes."""
    try:
        snapshot = json.loads(value)
        if (
            not isinstance(snapshot, dict)
            or type(snapshot.get("version")) is not int
            or snapshot["version"] != 1
            or not isinstance(snapshot.get("records"), list)
        ):
            raise ValueError("invalid snapshot header")
        records: list[OaiRecord] = []
        seen_ids: set[str] = set()
        for item in snapshot["records"]:
            if not isinstance(item, dict) or set(item) != {
                "identifier", "title", "event_url", "year", "primary_url",
                "supplemental_urls", "formats",
            }:
                raise ValueError("invalid record fields")
            for key in ("identifier", "title", "event_url"):
                if not isinstance(item[key], str) or not item[key].strip():
                    raise ValueError(f"invalid {key}")
            if item["identifier"] in seen_ids or not _http_url(item["event_url"]):
                raise ValueError("invalid or duplicate record identity")
            if item["year"] is not None and type(item["year"]) is not int:
                raise ValueError("invalid record year")
            year_match = EVENT_YEAR_RE.search(urlsplit(item["event_url"]).path)
            expected_year = int(year_match.group(1)) if year_match is not None else None
            if item["year"] != expected_year:
                raise ValueError("record year disagrees with its landing URL")
            primary = item["primary_url"]
            if primary is not None and (not isinstance(primary, str) or not _http_url(primary)):
                raise ValueError("invalid primary URL")
            for key in ("supplemental_urls", "formats"):
                if not isinstance(item[key], list) or any(
                    not isinstance(entry, str) or not entry.strip() for entry in item[key]
                ):
                    raise ValueError(f"invalid {key}")
            if any(not _http_url(url) for url in item["supplemental_urls"]):
                raise ValueError("invalid supplemental URL")
            records.append(OaiRecord(
                identifier=item["identifier"], title=item["title"], event_url=item["event_url"],
                year=item["year"], primary_url=primary,
                supplemental_urls=tuple(item["supplemental_urls"]), formats=tuple(item["formats"]),
            ))
            seen_ids.add(item["identifier"])
        return records
    except (ValueError, TypeError, KeyError) as exc:
        raise OaiProtocolError("invalidCache", "cached metadata snapshot is invalid") from exc
