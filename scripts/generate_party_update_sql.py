import pymysql
import pandas as pd
from pathlib import Path

def main():
    # Connect to LocalWP MySQL
    conn = pymysql.connect(
        user="root",
        password="root",
        database="local",
        unix_socket="/Users/nicolaspavez/Library/Application Support/Local/run/0VfptgzbQ/mysql/mysqld.sock",
        charset="utf8mb4"
    )

    # Paths
    base = Path(__file__).resolve().parent.parent
    party_map = pd.read_csv(base / "mappings" / "party_map_2024.csv")

    # Optional: read candidate CSV (for reference)
    candidates = pd.read_csv(base / "csv" / "candidates_2024.csv")

    with conn.cursor() as cur:
        updates = 0

        for _, row in party_map.iterrows():
            pact_label = row["pact_label"]
            party_id = int(row["party_id"])

            # Find all matching candidacies that belong to this pact label
            cur.execute("""
                UPDATE wp_politeia_candidacies c
                JOIN wp_politeia_political_parties p ON c.party_id = p.id
                SET c.party_id = %s
                WHERE c.alliance = %s;
            """, (party_id, pact_label))

            updates += cur.rowcount

        conn.commit()

    print(f"✅ Updated {updates} candidacies with correct party_id values.")

    conn.close()


if __name__ == "__main__":
    main()
