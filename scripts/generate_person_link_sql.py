import pymysql
import pandas as pd
from pathlib import Path

def main():
    # Connect to LocalWP MySQL via socket
    conn = pymysql.connect(
        user="root",
        password="root",
        database="local",
        unix_socket="/Users/nicolaspavez/Library/Application Support/Local/run/0VfptgzbQ/mysql/mysqld.sock",
        charset="utf8mb4"
    )

    base = Path(__file__).resolve().parent.parent
    cand_path = base / "csv" / "candidates_2024.csv"
    out_path = base / "sql" / "update_person_ids_2024.sql"

    df = pd.read_csv(cand_path)
    df["candidate_name"] = df["candidate_name"].str.strip()

    sql_lines = []
    matched, missing = 0, 0

    with conn.cursor() as cur:
        for _, row in df.iterrows():
            name = row["candidate_name"].strip()
            parts = name.split(" ", 1)
            given = parts[0]
            surname = parts[1] if len(parts) > 1 else ""

            cur.execute("""
                SELECT id FROM wp_politeia_people
                WHERE given_names LIKE %s AND (paternal_surname LIKE %s OR maternal_surname LIKE %s)
                LIMIT 1;
            """, (f"%{given}%", f"%{surname}%", f"%{surname}%"))
            res = cur.fetchone()

            if res:
                person_id = res[0]
                sql_lines.append(
                    f"UPDATE wp_politeia_candidacies SET person_id={person_id} "
                    f"WHERE election_id={int(row['election_id'])} AND votes={int(row['votes'])};"
                )
                matched += 1
            else:
                missing += 1

    conn.close()
    out_path.write_text("\n".join(sql_lines), encoding="utf-8")
    print(f"✅ Generated {matched} person link updates → {out_path}")
    print(f"⚠️  {missing} candidates not matched (check name variations).")

if __name__ == "__main__":
    main()
