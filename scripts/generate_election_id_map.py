"""Build the mapping between CSV rows and WordPress election IDs."""

import csv
import unicodedata
from pathlib import Path

from . import sql_parser

INPUT_CSV = Path("csv/elections_2024.csv")
OUTPUT_CSV = Path("mappings/election_id_map_2024.csv")


def _normalize(value: str) -> str:
    normalized = unicodedata.normalize("NFD", value)
    return "".join(ch for ch in normalized if not unicodedata.combining(ch)).lower()


def _build_office_index():
    offices = sql_parser.load_offices()
    index = {}
    for office in offices.values():
        index[_normalize(office.title)] = office
    return index


def _build_jurisdiction_index():
    jurisdictions = sql_parser.load_jurisdictions()
    by_code = {}
    for jurisdiction in jurisdictions.values():
        if jurisdiction.type != "COMMUNE":
            continue
        if jurisdiction.external_code:
            by_code.setdefault(jurisdiction.external_code, []).append(jurisdiction)
    return by_code


def _build_election_indexes():
    elections = sql_parser.load_elections()
    direct = {}
    by_name = {}
    for election in elections:
        key = (election.office_id, election.jurisdiction_id, election.election_date)
        direct[key] = election
        if election.name:
            name_key = (election.office_id, election.election_date, _normalize(election.name))
            by_name[name_key] = election
    return direct, by_name


def main() -> None:
    if not INPUT_CSV.exists():
        raise SystemExit(f"Missing input CSV: {INPUT_CSV}")

    INPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)

    offices = _build_office_index()
    jurisdictions_by_code = _build_jurisdiction_index()
    elections_direct, elections_by_name = _build_election_indexes()

    with INPUT_CSV.open("r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)

    output_rows = []
    for row in rows:
        office_name = row["office"].strip()
        office = offices.get(_normalize(office_name))
        if not office:
            raise SystemExit(f"Unknown office '{office_name}' in row {row}")

        commune_code = row["commune_code"].strip()
        candidates = jurisdictions_by_code.get(commune_code)
        if not candidates:
            raise SystemExit(f"Unknown commune code '{commune_code}' in row {row}")
        if len(candidates) == 1:
            jurisdiction = candidates[0]
        else:
            target_name = _normalize(row["commune_name"].strip())
            matches = [
                j
                for j in candidates
                if _normalize(j.common_name) == target_name
                or _normalize(j.official_name) == target_name
                or (
                    j.official_name.lower().startswith("comuna de ")
                    and _normalize(j.official_name[10:]) == target_name
                )
            ]
            if not matches:
                raise SystemExit(
                    f"Ambiguous commune code '{commune_code}' for row {row}"
                )
            unique_matches = {match.id: match for match in matches}
            jurisdiction = sorted(unique_matches.values(), key=lambda j: j.id)[0]

        election_date = row["election_date"].strip()
        direct_key = (office.id, jurisdiction.id, election_date)
        election = elections_direct.get(direct_key)
        if not election:
            normalized_names = {
                _normalize(jurisdiction.common_name),
                _normalize(jurisdiction.official_name),
            }
            if jurisdiction.official_name.lower().startswith("comuna de "):
                normalized_names.add(_normalize(jurisdiction.official_name[10:]))

            matches = [
                elections_by_name.get((office.id, election_date, name))
                for name in normalized_names
            ]
            matches = [match for match in matches if match]
            matches = list({match.id: match for match in matches}.values())

            if len(matches) == 1:
                election = matches[0]
            elif not matches:
                raise SystemExit(
                    "No matching election for row {}".format(row)
                )
            else:
                raise SystemExit(
                    "Multiple matching elections for row {} -> {}".format(
                        row, [match.id for match in matches]
                    )
                )

        output_rows.append(
            {
                "csv_row_id": row.get("csv_row_id") or str(len(output_rows) + 1),
                "commune_name": row["commune_name"].strip(),
                "commune_code": commune_code,
                "office": office.title,
                "election_date": election_date,
                "wp_election_id": str(election.id),
            }
        )

    with OUTPUT_CSV.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "csv_row_id",
                "commune_name",
                "commune_code",
                "office",
                "election_date",
                "wp_election_id",
            ],
        )
        writer.writeheader()
        writer.writerows(output_rows)


if __name__ == "__main__":
    main()
