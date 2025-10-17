import pymysql
import pandas as pd
from pathlib import Path
from difflib import get_close_matches

def main():
    conn = pymysql.connect(
        user="root",
        password="root",
        database="local",
        unix_socket="/Users/nicolaspavez/Library/Application Support/Local/run/0VfptgzbQ/mysql/mysqld.sock",
        charset="utf8mb4"
    )

    base = Path(__file__).resolve().parent.parent
    cand_path = base / "csv" / "candidates_2024.csv"
    people = pd.read_sql("SELECT id, given_names, paternal_surname, maternal_surname FROM wp_politeia_people", conn)
    candidacies = pd.read_sql("SELECT id, election_id, person_id FROM wp_politeia_candidacies WHERE person_id IS NULL", conn)

    df = pd.read_csv(cand_path)
    df = df[df["election_id"].isin(candidacies["election_id"])]

    updates = []
    unmatched = []

    for _, row in df.iterrows():
        name = row["candidate_name"]
        candidates = [f"{r.given_names} {r.paternal_surname or ''}".strip() for _, r in people.iterrows()]
        matches = get_close_matches(name, candidates, n=1, cutoff=0.85)
        if matches:
            match = matches[0]
            person_id = people.loc[candidates.index(match), "id"]
            updates.append(f"UPDATE wp_politeia_candidacies SET person_id={int(person_id)} WHERE election_id={int(row['election_id'])} AND votes={int(row['votes'])};")
        else:
            unmatched.append(name)

    out_path = base / "sql" / "update_fuzzy_person_ids_2024.sql"
    out_path.write_text("\n".join(updates))
    conn.close()

    print(f"✅ Generated {len(updates)} fuzzy person link updates → {out_path}")
    print(f"⚠️  {len(unmatched)} candidates remain unmatched")

if __name__ == "__main__":
    main()
