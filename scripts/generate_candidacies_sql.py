import pandas as pd
from pathlib import Path

def main():
    base = Path("/Users/nicolaspavez/Desktop/PoliteiaDB")
    map_path = base / "mappings" / "election_id_map_2024.csv"
    cand_path = base / "csv" / "candidates_2024.csv"
    out_path = base / "sql" / "candidacies_2024.sql"

    mapping = pd.read_csv(map_path)
    candidates = pd.read_csv(cand_path)

    merged = candidates.merge(mapping, left_on="election_id", right_on="csv_row_id", how="left")

    sql_lines = []
    for _, row in merged.iterrows():
        if pd.isna(row["wp_election_id"]):
            continue

        sql_lines.append(f"""
INSERT INTO wp_politeia_candidacies
    (election_id, person_id, party_id, alliance, list_position,
     votes, vote_share, elected, result_rank, source_url)
VALUES
    ({int(row['wp_election_id'])}, NULL, NULL, '{row['pact_label'].replace("'", "''")}', NULL,
     {int(row['votes'])}, {float(row['vote_share'])}, {int(row['is_winner'])}, NULL,
     'https://www.emol.com/especiales/2024/nacional/elecciones/municipales/')
ON DUPLICATE KEY UPDATE
    votes = VALUES(votes),
    vote_share = VALUES(vote_share),
    elected = VALUES(elected),
    alliance = VALUES(alliance),
    updated_at = CURRENT_TIMESTAMP;
""")

    out_path.write_text("\n".join(sql_lines))
    print(f"✅ Generated {len(sql_lines)} candidacies SQL statements → {out_path}")

if __name__ == "__main__":
    main()
