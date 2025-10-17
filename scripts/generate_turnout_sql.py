import pandas as pd
from pathlib import Path
import math

def sql_val(v, is_float=False):
    """Convert a Python value to a safe SQL literal."""
    if pd.isna(v) or v == "" or v is None:
        return "NULL"
    try:
        if is_float:
            return str(float(v))
        else:
            # If it's an integer-valued float, cast down to int
            f = float(v)
            if f.is_integer():
                return str(int(f))
            return str(f)
    except Exception:
        return "NULL"

def main():
    base_path = Path(__file__).resolve().parent.parent
    map_path = base_path / "mappings" / "election_id_map_2024.csv"
    part_path = base_path / "csv" / "participation_2024.csv"
    output_path = base_path / "sql" / "turnout_2024.sql"

    # --- Load CSVs ---
    mapping = pd.read_csv(map_path)
    participation = pd.read_csv(part_path)

    # --- Merge by election_id ---
    merged = participation.merge(mapping, left_on="election_id", right_on="csv_row_id", how="left")

    # --- Generate SQL ---
    sql_lines = []
    for _, row in merged.iterrows():
        election_id     = sql_val(row.get("wp_election_id"))
        valid_votes     = sql_val(row.get("valid_votes"))
        blank_votes     = sql_val(row.get("blank_votes"))
        null_votes      = sql_val(row.get("null_votes"))
        registered      = sql_val(row.get("padron"))
        actual          = sql_val(row.get("votantes"))
        turnout_percent = sql_val(row.get("participation_pct"), is_float=True)
        source_url      = "'https://www.emol.com/especiales/2024/nacional/elecciones/municipales/'"

        sql_lines.append(f"""
INSERT INTO wp_politeia_turnout
    (election_id, valid_votes, blank_votes, null_votes,
     registered_voters, actual_voters, turnout_percent, source_url)
VALUES
    ({election_id}, {valid_votes}, {blank_votes}, {null_votes},
     {registered}, {actual}, {turnout_percent}, {source_url})
ON DUPLICATE KEY UPDATE
    valid_votes = VALUES(valid_votes),
    blank_votes = VALUES(blank_votes),
    null_votes = VALUES(null_votes),
    registered_voters = VALUES(registered_voters),
    actual_voters = VALUES(actual_voters),
    turnout_percent = VALUES(turnout_percent);
""")

    # --- Save to file ---
    output_path.write_text("\n".join(sql_lines), encoding="utf-8")
    print(f"✅ Generated {len(sql_lines)} turnout SQL statements → {output_path}")

if __name__ == "__main__":
    main()
