"""Parse and normalize arXiv metadata returned by OAI-PMH."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import UTC, date, datetime
from email.utils import parsedate_to_datetime
from typing import Final

from lxml import etree  # pyright: ignore[reportAttributeAccessIssue]
from pydantic import BaseModel, ConfigDict

from arxiv_papers.oai import OAIRecord

ARXIV_ID_VERSION_RE: Final = re.compile(
    r"^(?P<id>.+?)(?:v(?P<version>[1-9][0-9]*))?$", re.IGNORECASE
)
NEW_ID_RE: Final = re.compile(r"^[0-9]{4}\.[0-9]{4,5}$")
OLD_ID_RE: Final = re.compile(r"^[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)*/[0-9]{7}$")
DOI_PREFIX_RE: Final = re.compile(
    r"^(?:doi:\s*|https?://(?:dx\.)?doi\.org/)", re.IGNORECASE
)


class MetadataParseError(Exception):
    """Raised when an OAI record cannot be converted to a paper record."""


class ArxivAuthor(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    keyname: str | None = None
    forenames: str | None = None
    suffix: str | None = None
    affiliations: tuple[str, ...] = ()
    display_name: str


class ArxivCategory(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    name: str
    is_primary: bool = False


class ArxivVersionHistory(BaseModel):
    """Version facts exposed by the official ``arXivRaw`` format."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    version: int
    submitted_at: datetime
    size_kb: int | None = None
    source_type: str | None = None


class ArxivPaperMetadata(BaseModel):
    """Normalized metadata for one arXiv version."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    arxiv_id: str
    version: int | None = None
    title: str = ""
    abstract: str = ""
    authors: tuple[ArxivAuthor, ...] = ()
    categories: tuple[ArxivCategory, ...] = ()
    created: date | None = None
    updated: date | None = None
    comments: str | None = None
    journal_ref: str | None = None
    doi: str | None = None
    license: str | None = None
    report_numbers: tuple[str, ...] = ()
    authors_raw: str | None = None
    version_history: tuple[ArxivVersionHistory, ...] = ()

    @property
    def first_submitted_at(self) -> datetime | None:
        if self.version_history:
            return min(item.submitted_at for item in self.version_history)
        if self.created is None:
            return None
        return datetime.combine(self.created, datetime.min.time(), tzinfo=UTC)

    @property
    def first_submitted_date(self) -> date | None:
        submitted_at = self.first_submitted_at
        return submitted_at.date() if submitted_at else self.created

    @property
    def primary_category(self) -> str | None:
        return next(
            (category.name for category in self.categories if category.is_primary), None
        )

    @property
    def metadata_hash(self) -> str:
        """Hash canonical normalized fields for stable deduplication."""

        canonical = json.dumps(
            self.model_dump(mode="json", exclude_none=True),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return hashlib.sha256(canonical).hexdigest()


class ParsedArxivRecord(BaseModel):
    """A parsed OAI record, including source identity and recoverable warnings."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    oai_identifier: str
    oai_datestamp: str | None
    deleted: bool = False
    metadata: ArxivPaperMetadata | None = None
    metadata_hash: str | None = None
    warnings: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class _XMLFields:
    title: str
    abstract: str
    authors: tuple[ArxivAuthor, ...]
    categories: tuple[ArxivCategory, ...]
    created: date | None
    updated: date | None
    comments: str | None
    journal_ref: str | None
    doi: str | None
    license: str | None
    report_numbers: tuple[str, ...]
    authors_raw: str | None
    version_history: tuple[ArxivVersionHistory, ...]
    warnings: tuple[str, ...]


def parse_oai_record(record: OAIRecord) -> ParsedArxivRecord:
    """Parse one OAI record without discarding its OAI-level identity."""

    if record.deleted:
        raw_id = record.identifier.rsplit(":", maxsplit=1)[-1]
        try:
            normalize_arxiv_id(raw_id)
        except MetadataParseError as exc:
            raise MetadataParseError(
                f"deleted record {record.identifier} has invalid arXiv id"
            ) from exc
        return ParsedArxivRecord(
            oai_identifier=record.identifier,
            oai_datestamp=record.datestamp,
            deleted=True,
        )
    if record.metadata_xml is None:
        raise MetadataParseError(f"record {record.identifier} has no metadata")

    try:
        root = etree.fromstring(
            record.metadata_xml,
            parser=etree.XMLParser(
                resolve_entities=False,
                no_network=True,
                recover=False,
                huge_tree=False,
            ),
        )
    except (etree.XMLSyntaxError, ValueError) as exc:
        raise MetadataParseError(
            f"record {record.identifier} has invalid metadata XML: {exc}"
        ) from exc

    id_element = _find_child(root, "id")
    raw_id = _text(id_element)
    if raw_id is None:
        raise MetadataParseError(f"record {record.identifier} metadata has no arXiv id")
    arxiv_id, version, id_warnings = normalize_arxiv_id(raw_id)
    fields = _parse_fields(root)
    current_version = version or (
        fields.version_history[-1].version if fields.version_history else None
    )
    warnings = (*id_warnings, *fields.warnings)
    metadata = ArxivPaperMetadata(
        arxiv_id=arxiv_id,
        version=current_version,
        title=fields.title,
        abstract=fields.abstract,
        authors=fields.authors,
        categories=fields.categories,
        created=fields.created,
        updated=fields.updated,
        comments=fields.comments,
        journal_ref=fields.journal_ref,
        doi=fields.doi,
        license=fields.license,
        report_numbers=fields.report_numbers,
        authors_raw=fields.authors_raw,
        version_history=fields.version_history,
    )
    return ParsedArxivRecord(
        oai_identifier=record.identifier,
        oai_datestamp=record.datestamp,
        metadata=metadata,
        metadata_hash=metadata.metadata_hash,
        warnings=warnings,
    )


def normalize_arxiv_id(raw_id: str) -> tuple[str, int | None, tuple[str, ...]]:
    """Return a canonical arXiv ID, version and normalization warnings."""

    value = normalize_whitespace(raw_id).strip()
    value = re.sub(r"^https?://arxiv\.org/(?:abs|pdf)/", "", value, flags=re.IGNORECASE)
    value = re.sub(r"^arxiv:\s*", "", value, flags=re.IGNORECASE)
    match = ARXIV_ID_VERSION_RE.fullmatch(value)
    if match is None:
        raise MetadataParseError(f"invalid arXiv id: {raw_id!r}")
    arxiv_id = match.group("id")
    version_text = match.group("version")
    version = int(version_text) if version_text is not None else None
    warnings: list[str] = []
    if (
        version is not None
        and version_text is not None
        and value.lower().endswith(f"v{version_text}")
    ):
        warnings.append("version suffix normalized from metadata id")
    if not (NEW_ID_RE.fullmatch(arxiv_id) or OLD_ID_RE.fullmatch(arxiv_id)):
        warnings.append(f"unrecognized arXiv id format: {arxiv_id}")
    return arxiv_id, version, tuple(warnings)


def normalize_whitespace(value: str) -> str:
    return " ".join(value.split())


def _parse_fields(root: etree._Element) -> _XMLFields:
    warnings: list[str] = []
    title = _normalized_field(root, "title")
    abstract = _normalized_field(root, "abstract")
    if not title:
        warnings.append("missing title")
    if not abstract:
        warnings.append("missing abstract")

    authors, authors_raw = _parse_authors(root, warnings)
    categories = _parse_categories(root, warnings)
    version_history = _parse_version_history(root, warnings)
    created, updated = _parse_dates(root, version_history, warnings)

    return _XMLFields(
        title=title,
        abstract=abstract,
        authors=authors,
        categories=categories,
        created=created,
        updated=updated,
        comments=_optional_text(root, "comments"),
        journal_ref=_optional_text(root, "journal-ref"),
        doi=_normalize_doi(_optional_text(root, "doi")),
        license=_optional_text(root, "license"),
        report_numbers=_split_optional_field(root, "report-no"),
        authors_raw=authors_raw,
        version_history=version_history,
        warnings=tuple(warnings),
    )


def _parse_authors(
    root: etree._Element, warnings: list[str]
) -> tuple[tuple[ArxivAuthor, ...], str | None]:
    authors: list[ArxivAuthor] = []
    authors_raw: str | None = None
    authors_element = _find_child(root, "authors")
    if authors_element is not None:
        author_elements = _children(authors_element, "author")
        if author_elements:
            for index, author_element in enumerate(author_elements, start=1):
                author = _parse_author(author_element)
                if author is None:
                    warnings.append(f"author {index} has no usable name")
                else:
                    authors.append(author)
        else:
            authors_raw = _text(authors_element)
            if authors_raw:
                authors.extend(_parse_raw_authors(authors_raw))
    if not authors:
        warnings.append("missing authors")
    return tuple(authors), authors_raw


def _parse_categories(
    root: etree._Element, warnings: list[str]
) -> tuple[ArxivCategory, ...]:
    categories_text = _normalized_field(root, "categories")
    categories = tuple(
        ArxivCategory(name=category, is_primary=index == 0)
        for index, category in enumerate(
            categories_text.split() if categories_text else ()
        )
    )
    if not categories:
        warnings.append("missing categories")
    return categories


def _parse_dates(
    root: etree._Element,
    version_history: tuple[ArxivVersionHistory, ...],
    warnings: list[str],
) -> tuple[date | None, date | None]:
    created, created_warning = _parse_date_field(root, "created")
    if created is None and version_history:
        created = version_history[0].submitted_at.date()
        created_warning = None
    updated, updated_warning = _parse_date_field(root, "updated")
    if updated is None and version_history:
        updated = version_history[-1].submitted_at.date()
        updated_warning = None
    if created_warning:
        warnings.append(created_warning)
    if updated_warning:
        warnings.append(updated_warning)
    return created, updated


def _parse_raw_authors(value: str) -> list[ArxivAuthor]:
    return [
        ArxivAuthor(display_name=name.strip())
        for name in re.split(r"\s+and\s+", value)
        if name.strip()
    ]


def _parse_version_history(
    root: etree._Element, warnings: list[str]
) -> tuple[ArxivVersionHistory, ...]:
    versions: list[ArxivVersionHistory] = []
    for element in _children(root, "version"):
        value = element.get("version")
        if value is None:
            warnings.append("version is missing version attribute")
            continue
        match = re.fullmatch(r"v([1-9][0-9]*)", value.strip(), flags=re.IGNORECASE)
        if match is None:
            warnings.append(f"invalid version value: {value}")
            continue
        date_text = _optional_text(element, "date")
        if date_text is None:
            warnings.append(f"version {value} is missing date")
            continue
        try:
            submitted_at = parsedate_to_datetime(date_text)
            if submitted_at.tzinfo is None:
                submitted_at = submitted_at.replace(tzinfo=UTC)
            submitted_at = submitted_at.astimezone(UTC)
        except (TypeError, ValueError, OverflowError):
            warnings.append(f"invalid version {value} date: {date_text}")
            continue
        size_text = _optional_text(element, "size")
        try:
            size_kb = int(size_text.removesuffix("kb").strip()) if size_text else None
        except ValueError:
            size_kb = None
            warnings.append(f"invalid version {value} size: {size_text}")
        versions.append(
            ArxivVersionHistory(
                version=int(match.group(1)),
                submitted_at=submitted_at,
                size_kb=size_kb,
                source_type=_optional_text(element, "source_type"),
            )
        )
    versions.sort(key=lambda item: item.version)
    return tuple(versions)


def _parse_author(element: etree._Element) -> ArxivAuthor | None:
    keyname = _optional_text(element, "keyname")
    forenames = _optional_text(element, "forenames")
    suffix = _optional_text(element, "suffix")
    affiliations = tuple(
        normalize_whitespace("".join(child.itertext()))
        for child in _children(element, "affiliation")
    )
    name_parts = tuple(part for part in (forenames, keyname, suffix) if part)
    if not name_parts:
        return None
    return ArxivAuthor(
        keyname=keyname,
        forenames=forenames,
        suffix=suffix,
        affiliations=affiliations,
        display_name=" ".join(name_parts),
    )


def _parse_date_field(
    root: etree._Element, name: str
) -> tuple[date | None, str | None]:
    value = _optional_text(root, name)
    if value is None:
        return None, f"missing {name} date"
    try:
        return date.fromisoformat(value[:10]), None
    except ValueError:
        return None, f"invalid {name} date: {value}"


def _normalize_doi(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = DOI_PREFIX_RE.sub("", value).strip()
    return normalized or None


def _split_optional_field(root: etree._Element, name: str) -> tuple[str, ...]:
    value = _normalized_field(root, name)
    return tuple(value.split()) if value else ()


def _optional_text(root: etree._Element, name: str) -> str | None:
    value = _normalized_field(root, name)
    return value or None


def _normalized_field(root: etree._Element, name: str) -> str:
    element = _find_child(root, name)
    return (
        normalize_whitespace("".join(element.itertext())) if element is not None else ""
    )


def _text(element: etree._Element | None) -> str | None:
    if element is None:
        return None
    value = normalize_whitespace("".join(element.itertext()))
    return value or None


def _find_child(root: etree._Element, name: str) -> etree._Element | None:
    return next((child for child in root if _local_name(child) == name), None)


def _children(root: etree._Element, name: str) -> tuple[etree._Element, ...]:
    return tuple(child for child in root if _local_name(child) == name)


def _local_name(element: etree._Element) -> str:
    return etree.QName(element).localname


__all__ = [
    "ArxivAuthor",
    "ArxivCategory",
    "ArxivPaperMetadata",
    "ArxivVersionHistory",
    "MetadataParseError",
    "ParsedArxivRecord",
    "normalize_arxiv_id",
    "normalize_whitespace",
    "parse_oai_record",
]
