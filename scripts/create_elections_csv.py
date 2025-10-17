"""Generate the public CSV used for the 2024 election mapping tasks."""

import csv
import unicodedata
from pathlib import Path

from . import sql_parser

CSV_PATH = Path("csv/elections_2024.csv")


def _normalize(value: str) -> str:
    normalized = unicodedata.normalize("NFD", value)
    return "".join(ch for ch in normalized if not unicodedata.combining(ch)).lower()


def _build_jurisdiction_index():
    jurisdictions = sql_parser.load_jurisdictions()
    index = {}
    for jurisdiction in jurisdictions.values():
        if jurisdiction.type != "COMMUNE" or not jurisdiction.external_code:
            continue
        names = {jurisdiction.common_name, jurisdiction.official_name}
        if jurisdiction.official_name.lower().startswith("comuna de "):
            names.add(jurisdiction.official_name[10:])
        for name in names:
            index[_normalize(name)] = jurisdiction
    return index


def main() -> None:
    CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    offices = sql_parser.load_offices()
    elections = sql_parser.load_elections()
    jurisdiction_index = _build_jurisdiction_index()

    rows = []
    for election in elections:
        if election.election_date[:4] != "2024":
            continue
        office = offices.get(election.office_id)
        if office is None:
            continue
        if not election.name:
            continue
        key = _normalize(election.name)
        jurisdiction = jurisdiction_index.get(key)
        if not jurisdiction:
            continue
        rows.append(
            {
                "commune_name": jurisdiction.common_name,
                "commune_code": jurisdiction.external_code,
                "office": office.title,
                "election_date": election.election_date,
                "wp_election_id": election.id,
            }
        )

    rows.sort(key=lambda entry: (entry["office"], entry["commune_name"]))

    with CSV_PATH.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "csv_row_id",
                "commune_name",
                "commune_code",
                "office",
                "election_date",
            ],
        )
        writer.writeheader()
        for idx, row in enumerate(rows, start=1):
            writer.writerow(
                {
                    "csv_row_id": idx,
                    "commune_name": row["commune_name"],
                    "commune_code": row["commune_code"],
                    "office": row["office"],
                    "election_date": row["election_date"],
                }
            )


if __name__ == "__main__":
    main()
