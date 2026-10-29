from database import get_db_connection


def bulk_ingest_votes(start_congress: int = 116, end_congress: int = 119):
    """
    Bulk loads member votes and bio data directly into MotherDuck/DuckDB.
    Defaults to Congresses 116 through 119 (~8 years of legislative voting history).
    """
    conn = get_db_connection()

    print(f"1. Bulk loading Voteview votes for Congresses {start_congress} through {end_congress}...")

    conn.execute(f"""
        CREATE OR REPLACE TABLE main.fact_member_votes AS
        SELECT
            CONCAT(v.congress, '_', LOWER(v.chamber), '_', v.rollnumber) AS vote_id,
            v.congress,
            LOWER(v.chamber) AS chamber,
            m.bioguide_id AS member_id,
            CASE
                WHEN v.cast_code IN (1, 2, 3) THEN 'Yea'
                WHEN v.cast_code IN (4, 5, 6) THEN 'Nay'
                WHEN v.cast_code IN (7, 8) THEN 'Present'
                WHEN v.cast_code = 9 THEN 'Not Voting'
                ELSE 'Other'
            END AS vote_position,
            'bulk_voteview' AS data_source,
            CURRENT_TIMESTAMP AS ingested_at
        FROM read_csv_auto('https://voteview.com/static/data/out/votes/HSall_votes.csv') v
        JOIN read_csv_auto('https://voteview.com/static/data/out/members/HSall_members.csv') m
          ON v.icpsr = m.icpsr AND v.chamber = m.chamber AND v.congress = m.congress
        WHERE v.congress BETWEEN {start_congress} AND {end_congress};
    """)

    # Primary key constraint for fast lookup and conflict resolution
    conn.execute("""
                 ALTER TABLE main.fact_member_votes
                     ADD PRIMARY KEY (vote_id, member_id);
                 """)

    print("2. Rebuilding attendance & party loyalty views across multi-congress history...")

    # 2a. Rebuild Attendance View
    conn.execute("""
                 CREATE OR REPLACE VIEW main.mart_member_attendance AS
                 SELECT
                     member_id,
                     COUNT(vote_id) AS total_eligible_votes,
                     COUNT(CASE WHEN vote_position = 'Not Voting' THEN 1 END) AS missed_votes,
                     COUNT(CASE WHEN vote_position = 'Present' THEN 1 END) AS present_abstain_votes,
                     ROUND(
                             (COUNT(CASE WHEN vote_position = 'Not Voting' THEN 1 END)::FLOAT / NULLIF(COUNT(vote_id), 0)) * 100,
                             2
                     ) AS missed_vote_pct
                 FROM main.fact_member_votes
                 GROUP BY member_id;
                 """)

    # 2b. Rebuild Party Loyalty View (aggregates across all loaded congresses per member)
    conn.execute("""
                 CREATE OR REPLACE VIEW main.mart_party_loyalty AS
        WITH vote_parties AS (
            SELECT 
                v.vote_id,
                v.member_id,
                v.vote_position,
                m.party AS member_party
            FROM main.fact_member_votes v
            JOIN main.raw_members m ON v.member_id = m.bioguide_id
            WHERE v.vote_position IN ('Yea', 'Nay')
        ),
        party_majority_positions AS (
            SELECT 
                vote_id,
                member_party,
                CASE 
                    WHEN COUNT(CASE WHEN vote_position = 'Yea' THEN 1 END) > 
                         COUNT(CASE WHEN vote_position = 'Nay' THEN 1 END) 
                    THEN 'Yea' 
                    ELSE 'Nay' 
                END AS party_majority_stance
            FROM vote_parties
            GROUP BY vote_id, member_party
        ),
        party_unity_votes AS (
            SELECT d.vote_id
            FROM party_majority_positions d
            JOIN party_majority_positions r 
              ON d.vote_id = r.vote_id 
             AND d.member_party != r.member_party
            WHERE d.party_majority_stance != r.party_majority_stance
            GROUP BY d.vote_id
        )
                 SELECT
                     vp.member_id,
                     COUNT(vp.vote_id) AS total_party_unity_votes,
                     COUNT(CASE WHEN vp.vote_position = pmp.party_majority_stance THEN 1 END) AS votes_with_party,
                     ROUND(
                             (COUNT(CASE WHEN vp.vote_position = pmp.party_majority_stance THEN 1 END)::FLOAT / 
                 NULLIF(COUNT(vp.vote_id), 0)) * 100,
                             1
                     ) AS party_loyalty_pct
                 FROM vote_parties vp
                          JOIN party_unity_votes pu ON vp.vote_id = pu.vote_id
                          JOIN party_majority_positions pmp
                               ON vp.vote_id = pmp.vote_id
                                   AND vp.member_party = pmp.member_party
                 GROUP BY vp.member_id;
                 """)

    conn.close()
    print("Multi-Congress bulk hydration complete!")

if __name__ == "__main__":
    bulk_ingest_votes()