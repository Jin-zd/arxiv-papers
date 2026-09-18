"""Local scope filters applied after OAI-PMH metadata parsing."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from arxiv_papers.metadata import ParsedArxivRecord

DEFAULT_CREATED_FROM = date(2023, 1, 1)
DEFAULT_CATEGORY_PREFIXES = ("cs.",)


@dataclass(frozen=True, slots=True)
class PaperScope:
    """The local retention scope for a synchronized paper library."""

    created_from: date | None = DEFAULT_CREATED_FROM
    created_until: date | None = None
    category_prefixes: tuple[str, ...] = DEFAULT_CATEGORY_PREFIXES

    def __post_init__(self) -> None:
        if (
            self.created_from
            and self.created_until
            and self.created_until < self.created_from
        ):
            raise ValueError("created_until must not be earlier than created_from")
        if not self.category_prefixes:
            raise ValueError("category_prefixes must not be empty")
        if any(not prefix.strip() for prefix in self.category_prefixes):
            raise ValueError("category_prefixes must not contain empty values")

    def accepts(self, record: ParsedArxivRecord) -> bool:
        """Return whether a parsed record belongs to this local library."""

        if record.deleted:
            # A deleted record has no metadata from which its creation date or
            # categories can be recovered.  The selected OAI set is the safest
            # available scope for applying the deletion.
            return True
        metadata = record.metadata
        if metadata is None:
            return False
        created = metadata.first_submitted_date
        if created is None:
            return False
        if self.created_from and created < self.created_from:
            return False
        if self.created_until and created > self.created_until:
            return False
        return any(
            category.name == prefix or category.name.startswith(prefix)
            for category in metadata.categories
            for prefix in self.category_prefixes
        )


__all__ = [
    "DEFAULT_CATEGORY_PREFIXES",
    "DEFAULT_CREATED_FROM",
    "PaperScope",
]
