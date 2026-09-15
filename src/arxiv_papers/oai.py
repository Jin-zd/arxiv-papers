"""Client for reading arXiv metadata through OAI-PMH.

The client deliberately stops at the OAI-PMH boundary.  It does not know
about arXiv's metadata schema; callers receive the metadata XML so the next
pipeline stage can archive and parse it without losing information.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Final, Self

import httpx
from lxml import etree  # pyright: ignore[reportAttributeAccessIssue]

LOGGER = logging.getLogger(__name__)

OAI_NS: Final = "http://www.openarchives.org/OAI/2.0/"
DEFAULT_ENDPOINT: Final = "https://export.arxiv.org/oai2"
DEFAULT_METADATA_PREFIX: Final = "arXiv"
DEFAULT_USER_AGENT: Final = "arxiv-papers/0.1.0"


class OAIClientError(Exception):
    """Base exception for OAI-PMH client failures."""


class OAITransportError(OAIClientError):
    """A request failed after the configured retry policy was exhausted."""


class OAIHTTPError(OAIClientError):
    """The endpoint returned a non-success HTTP status."""

    def __init__(self, status_code: int, message: str, *, url: str) -> None:
        super().__init__(f"HTTP {status_code} from {url}: {message}")
        self.status_code = status_code
        self.url = url
        self.message = message


class OAIParseError(OAIClientError):
    """The endpoint response was not valid OAI-PMH XML."""


class OAIProtocolError(OAIClientError):
    """The endpoint returned an OAI-PMH ``error`` element."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(f"OAI-PMH error {code}: {message}")
        self.code = code
        self.message = message


@dataclass(frozen=True, slots=True)
class OAIClientConfig:
    """Runtime settings for :class:`OAIClient`."""

    endpoint: str = DEFAULT_ENDPOINT
    metadata_prefix: str = DEFAULT_METADATA_PREFIX
    user_agent: str = DEFAULT_USER_AGENT
    connect_timeout: float = 10.0
    read_timeout: float = 120.0
    write_timeout: float = 30.0
    pool_timeout: float = 30.0
    max_retries: int = 5
    backoff_factor: float = 1.0
    max_backoff: float = 60.0
    request_delay: float = 3.0
    max_response_bytes: int = 50 * 1024 * 1024

    def __post_init__(self) -> None:
        if not self.endpoint.startswith(("http://", "https://")):
            raise ValueError("endpoint must use http:// or https://")
        if not self.metadata_prefix.strip():
            raise ValueError("metadata_prefix must not be empty")
        if self.max_retries < 0:
            raise ValueError("max_retries must be non-negative")
        if self.backoff_factor < 0 or self.max_backoff < 0:
            raise ValueError("backoff values must be non-negative")
        if self.request_delay < 0:
            raise ValueError("request_delay must be non-negative")
        if self.max_response_bytes <= 0:
            raise ValueError("max_response_bytes must be positive")

    @property
    def timeout(self) -> httpx.Timeout:
        return httpx.Timeout(
            connect=self.connect_timeout,
            read=self.read_timeout,
            write=self.write_timeout,
            pool=self.pool_timeout,
        )


@dataclass(frozen=True, slots=True)
class OAIRecord:
    """A single OAI-PMH record with its metadata preserved as XML."""

    identifier: str
    datestamp: str | None
    metadata_xml: bytes | None
    deleted: bool = False


@dataclass(frozen=True, slots=True)
class OAIPage:
    """One successful ``ListRecords`` response."""

    raw_xml: bytes
    records: tuple[OAIRecord, ...]
    resumption_token: str | None
    cursor: int | None
    complete_list_size: int | None
    request_params: tuple[tuple[str, str], ...] = ()
    http_status: int = 200
    elapsed_seconds: float | None = None


def _local_name(element: etree._Element) -> str:
    """Return an XML element's local name, independent of its namespace."""

    return etree.QName(element).localname


def _child(element: etree._Element, name: str) -> etree._Element | None:
    for candidate in element:
        if _local_name(candidate) == name:
            return candidate
    return None


def _text(element: etree._Element | None) -> str | None:
    if element is None or element.text is None:
        return None
    value = element.text.strip()
    return value or None


def _parse_retry_after(value: str | None) -> float | None:
    """Parse an HTTP Retry-After value into seconds."""

    if not value:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        pass

    try:
        retry_at = parsedate_to_datetime(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if retry_at is None:
        return None
    if retry_at.tzinfo is None:
        return None
    return max(0.0, (retry_at - datetime.now(UTC)).total_seconds())


class OAIClient:
    """Synchronous, paginated OAI-PMH client.

    A client instance owns one ``httpx.Client`` and should be used as a
    context manager.  The iterator yields one page at a time, allowing the
    caller to archive and commit each page before requesting the next one.
    """

    def __init__(
        self,
        config: OAIClientConfig | None = None,
        *,
        http_client: httpx.Client | None = None,
    ) -> None:
        self.config = config or OAIClientConfig()
        self._owns_http_client = http_client is None
        self._http = http_client or httpx.Client(
            timeout=self.config.timeout,
            headers={"User-Agent": self.config.user_agent},
            follow_redirects=True,
        )
        self._last_request_at: float | None = None

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None:
        self.close()

    def close(self) -> None:
        if self._owns_http_client:
            self._http.close()

    def list_records(
        self,
        *,
        from_: str | None = None,
        until: str | None = None,
        set_name: str | None = None,
        metadata_prefix: str | None = None,
        resumption_token: str | None = None,
    ) -> Iterator[OAIPage]:
        """Yield all pages for one ``ListRecords`` request.

        The first request uses the supplied date/set parameters.  Once the
        server returns a resumption token, subsequent requests contain only
        that token, as required by OAI-PMH.
        """

        prefix = metadata_prefix or self.config.metadata_prefix
        if resumption_token is None:
            params: Mapping[str, str] = {
                "verb": "ListRecords",
                "metadataPrefix": prefix,
            }
            mutable_params = dict(params)
            if from_ is not None:
                mutable_params["from"] = from_
            if until is not None:
                mutable_params["until"] = until
            if set_name is not None:
                mutable_params["set"] = set_name
        else:
            mutable_params = {
                "verb": "ListRecords",
                "resumptionToken": resumption_token,
            }

        while True:
            page = self._request_page(mutable_params)
            yield page
            if not page.resumption_token:
                return
            mutable_params = {
                "verb": "ListRecords",
                "resumptionToken": page.resumption_token,
            }

    def _request_page(self, params: Mapping[str, str]) -> OAIPage:
        self._respect_request_delay()
        started_at = time.monotonic()
        url = (
            str(self._http.base_url.join(self.config.endpoint))
            if self._http.base_url
            else self.config.endpoint
        )
        response = self._request_with_retries(params)
        elapsed = time.monotonic() - started_at
        LOGGER.info(
            "oai_request_succeeded",
            extra={
                "oai_endpoint": url,
                "oai_params": dict(params),
                "http_status": response.status_code,
                "response_bytes": len(response.content),
                "elapsed_seconds": round(elapsed, 3),
            },
        )
        return self._parse_page(
            response.content,
            request_params=params,
            http_status=response.status_code,
            elapsed_seconds=elapsed,
        )

    def _request_with_retries(self, params: Mapping[str, str]) -> httpx.Response:
        attempts = self.config.max_retries + 1
        for attempt in range(1, attempts + 1):
            try:
                response = self._http.get(self.config.endpoint, params=dict(params))
            except httpx.RequestError as exc:
                if attempt == attempts:
                    raise OAITransportError(
                        f"request failed after {attempt} attempts: {exc}"
                    ) from exc
                delay = self._backoff_delay(attempt)
                LOGGER.warning(
                    "oai_request_retry",
                    extra={
                        "attempt": attempt,
                        "max_attempts": attempts,
                        "reason": str(exc),
                        "delay_seconds": delay,
                    },
                )
                time.sleep(delay)
                continue

            if response.status_code < 400:
                if len(response.content) > self.config.max_response_bytes:
                    raise OAITransportError(
                        f"response exceeds max_response_bytes={self.config.max_response_bytes}"
                    )
                return response

            if (
                not self._is_retryable_status(response.status_code)
                or attempt == attempts
            ):
                raise OAIHTTPError(
                    response.status_code,
                    response.text[:500],
                    url=str(response.request.url),
                )

            retry_after = _parse_retry_after(response.headers.get("Retry-After"))
            delay = min(
                self.config.max_backoff,
                retry_after
                if retry_after is not None
                else self._backoff_delay(attempt),
            )
            response.close()
            LOGGER.warning(
                "oai_request_retry",
                extra={
                    "attempt": attempt,
                    "max_attempts": attempts,
                    "status_code": response.status_code,
                    "delay_seconds": delay,
                },
            )
            time.sleep(delay)

        raise AssertionError("retry loop must return or raise")

    def _respect_request_delay(self) -> None:
        if self._last_request_at is not None:
            remaining = self.config.request_delay - (
                time.monotonic() - self._last_request_at
            )
            if remaining > 0:
                time.sleep(remaining)
        self._last_request_at = time.monotonic()

    def _backoff_delay(self, attempt: int) -> float:
        return min(
            self.config.max_backoff, self.config.backoff_factor * (2 ** (attempt - 1))
        )

    @staticmethod
    def _is_retryable_status(status_code: int) -> bool:
        return status_code == 408 or status_code == 429 or 500 <= status_code <= 599

    def _parse_page(
        self,
        raw_xml: bytes,
        *,
        request_params: Mapping[str, str] | None = None,
        http_status: int = 200,
        elapsed_seconds: float | None = None,
    ) -> OAIPage:
        try:
            root = etree.fromstring(
                raw_xml,
                parser=etree.XMLParser(
                    resolve_entities=False,
                    no_network=True,
                    recover=False,
                    huge_tree=False,
                ),
            )
        except (etree.XMLSyntaxError, ValueError) as exc:
            raise OAIParseError(f"invalid OAI-PMH XML: {exc}") from exc

        errors = [element for element in root.iter() if _local_name(element) == "error"]
        if errors:
            error = errors[0]
            raise OAIProtocolError(
                error.get("code", "unknown"),
                _text(error) or "the OAI-PMH endpoint returned an error",
            )

        list_records = next(
            (
                element
                for element in root.iter()
                if _local_name(element) == "ListRecords"
            ),
            None,
        )
        if list_records is None:
            raise OAIParseError("OAI-PMH response does not contain ListRecords")

        records: list[OAIRecord] = []
        for record in list_records:
            if _local_name(record) != "record":
                continue
            header = _child(record, "header")
            if header is None:
                raise OAIParseError("OAI-PMH record does not contain a header")
            identifier = _text(_child(header, "identifier"))
            if identifier is None:
                raise OAIParseError("OAI-PMH record header has no identifier")
            metadata = _child(record, "metadata")
            metadata_xml: bytes | None = None
            if metadata is not None and len(metadata):
                metadata_xml = etree.tostring(metadata[0], encoding="utf-8")
            records.append(
                OAIRecord(
                    identifier=identifier,
                    datestamp=_text(_child(header, "datestamp")),
                    metadata_xml=metadata_xml,
                    deleted=header.get("status") == "deleted",
                )
            )

        token_element = _child(list_records, "resumptionToken")
        token = _text(token_element)
        cursor: int | None = None
        complete_list_size: int | None = None
        if token_element is not None:
            cursor = _parse_non_negative_int(token_element.get("cursor"))
            complete_list_size = _parse_non_negative_int(
                token_element.get("completeListSize")
            )

        return OAIPage(
            raw_xml=raw_xml,
            records=tuple(records),
            resumption_token=token,
            cursor=cursor,
            complete_list_size=complete_list_size,
            request_params=tuple((request_params or {}).items()),
            http_status=http_status,
            elapsed_seconds=elapsed_seconds,
        )


def _parse_non_negative_int(value: str | None) -> int | None:
    if value is None:
        return None
    try:
        parsed = int(value)
    except ValueError:
        return None
    return parsed if parsed >= 0 else None


__all__ = [
    "DEFAULT_ENDPOINT",
    "DEFAULT_METADATA_PREFIX",
    "OAIClient",
    "OAIClientConfig",
    "OAIClientError",
    "OAIHTTPError",
    "OAIPage",
    "OAIParseError",
    "OAIProtocolError",
    "OAIRecord",
    "OAITransportError",
]
