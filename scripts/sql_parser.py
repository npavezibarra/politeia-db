"""Helpers to read data from the ``local (2).sql`` dump without MySQL."""

from __future__ import annotations

import pathlib
from dataclasses import dataclass
from typing import Dict, Iterable, Iterator, List, Tuple

SQL_FILENAME = pathlib.Path("local (2).sql")


class SQLParseError(RuntimeError):
    """Raised when an INSERT statement cannot be parsed correctly."""


@dataclass(frozen=True)
class Office:
    id: int
    title: str
    requires_scope: int
    allowed_scope: str
    description: str | None


@dataclass(frozen=True)
class Jurisdiction:
    id: int
    official_name: str
    common_name: str
    type: str
    parent_id: int | None
    external_code: str | None


@dataclass(frozen=True)
class Election:
    id: int
    office_id: int
    jurisdiction_id: int
    election_date: str
    title: str
    name: str


def _convert_non_string(token: str) -> object:
    if token == "" or token.upper() == "NULL":
        return None
    try:
        return int(token)
    except ValueError:
        try:
            return float(token)
        except ValueError:
            raise SQLParseError(f"Unsupported literal: {token}")


def _parse_row(row_text: str) -> Tuple[object, ...]:
    if not row_text.startswith("(") or not row_text.endswith(")"):
        raise SQLParseError(f"Row does not have tuple delimiters: {row_text}")

    inner = row_text[1:-1]
    values: List[object] = []
    token_chars: List[str] = []
    idx = 0
    length = len(inner)

    def flush_token() -> None:
        if token_chars:
            token = "".join(token_chars).strip()
            token_chars.clear()
        else:
            token = ""
        values.append(_convert_non_string(token))

    while idx < length:
        ch = inner[idx]
        if ch.isspace():
            idx += 1
            continue

        if ch == ',':
            flush_token()
            idx += 1
            continue

        if ch == "'":
            idx += 1
            chars: List[str] = []
            while idx < length:
                ch = inner[idx]
                if ch == "\\":
                    idx += 1
                    if idx >= length:
                        raise SQLParseError("Dangling escape in string literal")
                    chars.append(inner[idx])
                    idx += 1
                    continue
                if ch == "'":
                    if idx + 1 < length and inner[idx + 1] == "'":
                        chars.append("'")
                        idx += 2
                        continue
                    idx += 1
                    break
                chars.append(ch)
                idx += 1
            else:
                raise SQLParseError("Unterminated string literal")

            values.append("".join(chars))
            token_chars.clear()

            while idx < length and inner[idx].isspace():
                idx += 1
            if idx < length and inner[idx] == ',':
                idx += 1
            continue

        token_chars.append(ch)
        idx += 1

    if token_chars:
        flush_token()

    return tuple(values)


def _iter_insert_rows(sql_lines: Iterable[str]) -> Iterator[Tuple[object, ...]]:
    buffer: List[str] = []
    for raw_line in sql_lines:
        line = raw_line.strip()
        if not line:
            continue
        buffer.append(line)
        if line.endswith(",") or line.endswith(";"):
            row_text = " ".join(buffer).rstrip(",;")
            buffer.clear()
            yield _parse_row(row_text)


def _extract_insert_block(sql_path: pathlib.Path, table_name: str) -> Iterator[Tuple[object, ...]]:
    insert_prefix = f"INSERT INTO `{table_name}`"
    collecting = False
    current_lines: List[str] = []

    with sql_path.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            if not collecting:
                if raw_line.startswith(insert_prefix):
                    collecting = True
                continue

            if raw_line.lstrip().startswith("("):
                current_lines.append(raw_line)
            elif collecting:
                if current_lines:
                    yield from _iter_insert_rows(current_lines)
                    current_lines.clear()
                if raw_line.startswith("INSERT INTO"):
                    collecting = raw_line.startswith(insert_prefix)
                else:
                    collecting = False

        if collecting and current_lines:
            yield from _iter_insert_rows(current_lines)


def load_offices(sql_path: pathlib.Path = SQL_FILENAME) -> Dict[int, Office]:
    offices: Dict[int, Office] = {}
    for row in _extract_insert_block(sql_path, "wp_politeia_offices"):
        office = Office(
            id=int(row[0]),
            title=row[1],
            requires_scope=int(row[2]),
            allowed_scope=row[3],
            description=row[4],
        )
        offices[office.id] = office
    return offices


def load_jurisdictions(sql_path: pathlib.Path = SQL_FILENAME) -> Dict[int, Jurisdiction]:
    jurisdictions: Dict[int, Jurisdiction] = {}
    for row in _extract_insert_block(sql_path, "wp_politeia_jurisdictions"):
        jurisdiction = Jurisdiction(
            id=int(row[0]),
            official_name=row[1],
            common_name=row[2],
            type=row[3],
            parent_id=None if row[4] is None else int(row[4]),
            external_code=row[5],
        )
        jurisdictions[jurisdiction.id] = jurisdiction
    return jurisdictions


def load_elections(sql_path: pathlib.Path = SQL_FILENAME) -> List[Election]:
    elections: List[Election] = []
    for row in _extract_insert_block(sql_path, "wp_politeia_elections"):
        elections.append(
            Election(
                id=int(row[0]),
                office_id=int(row[2]),
                jurisdiction_id=int(row[3]),
                election_date=row[4],
                title=row[5],
                name=row[6],
            )
        )
    return elections
