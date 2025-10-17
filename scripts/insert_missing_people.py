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

    df = pd.read_csv(cand_path)
    df["candidate_name"] = df["candidate_name"].str.strip()

    inserted, skipped = 0, 0
    with conn.cursor() as cur:
        for name in df["candidate_name"].unique():
            parts = name.strip().split(" ", 1)
            given = parts[0]
            surname = parts[1] if len(parts) > 1 else ""

            # Check if person already exists
            cur.execute("""
                SELECT id FROM wp_politeia_people
                WHERE given_names LIKE %s AND (paternal_surname LIKE %s OR maternal_surname LIKE %s)
                LIMIT 1;
            """, (f"%{given}%", f"%{surname}%", f"%{surname}%"))
            res = cur.fetchone()

            if not res:
                cur.execute("""
                    INSERT INTO wp_politeia_people (given_names, paternal_surname, birth_date)
                    VALUES (%s, %s, '1900-01-01');
                """, (given, surname))
                inserted += 1
            else:
                skipped += 1

    conn.commit()
    conn.close()
    print(f"✅ Inserted {inserted} new people, skipped {skipped} existing.")

if __name__ == "__main__":
    main()
