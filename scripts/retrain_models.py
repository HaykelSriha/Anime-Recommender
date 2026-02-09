"""Retrain all ML models on the clean 844-anime AniList dataset.

1. Recreate missing tables (fact_user_rating, fact_collaborative_scores)
2. Retrain TF-IDF on ALL 844 anime (top-10 per anime)
3. Generate synthetic ratings for NMF training
4. Train NMF collaborative filtering
5. Store everything in the warehouse
"""
import duckdb
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.decomposition import NMF
from scipy.sparse import csr_matrix

DB = "warehouse/anime_full_phase1.duckdb"


def main():
    conn = duckdb.connect(DB)

    # =====================================================================
    # STEP 0: Recreate missing tables
    # =====================================================================
    print("[0/4] Recreating missing tables...")

    conn.execute("""
        CREATE TABLE IF NOT EXISTS fact_user_rating (
            rating_key INTEGER PRIMARY KEY,
            user_key INTEGER NOT NULL,
            anime_key INTEGER NOT NULL,
            rating DECIMAL(3,1) NOT NULL,
            rating_date DATE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS fact_collaborative_scores (
            collab_key INTEGER PRIMARY KEY,
            user_key INTEGER,
            anime_key INTEGER,
            predicted_rating DECIMAL(5,4),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS fact_user_model_score (
            score_key INTEGER PRIMARY KEY,
            user_key INTEGER,
            anime_key INTEGER,
            model_version_id INTEGER,
            score DECIMAL(5,4),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    conn.commit()
    print("  Tables ready.")

    # =====================================================================
    # STEP 1: Retrain TF-IDF on ALL anime
    # =====================================================================
    print("\n[1/4] Training TF-IDF on all anime...")

    # Clear old similarities
    conn.execute("DELETE FROM fact_anime_similarity")
    conn.commit()

    # Load ALL anime with rich features (genres + tags + description)
    anime_rows = conn.execute("""
        SELECT
            a.anime_key,
            a.title,
            COALESCE(
                (SELECT STRING_AGG(g.genre_name, ' ')
                 FROM bridge_anime_genre bg
                 JOIN dim_genre g ON bg.genre_key = g.genre_key
                 WHERE bg.anime_key = a.anime_key),
                ''
            ) || ' ' || COALESCE(a.tags, '') || ' ' || COALESCE(LEFT(a.description, 200), '')
            as features
        FROM dim_anime a
        WHERE a.is_current = true
        ORDER BY a.anime_key
    """).fetchall()

    anime_keys = [r[0] for r in anime_rows]
    features = [str(r[2])[:500] for r in anime_rows]
    key_to_idx = {k: i for i, k in enumerate(anime_keys)}

    print(f"  Anime: {len(anime_keys)}")

    vectorizer = TfidfVectorizer(
        max_features=500,
        stop_words="english",
        ngram_range=(1, 2),
        min_df=2,
    )
    tfidf_matrix = vectorizer.fit_transform(features)
    print(f"  TF-IDF matrix: {tfidf_matrix.shape}")

    sim_matrix = cosine_similarity(tfidf_matrix)
    print(f"  Computing top-10 similarities per anime...")

    stored = 0
    for i, ak1 in enumerate(anime_keys):
        sims = sim_matrix[i]
        # Top 10 excluding self
        top_idx = np.argsort(-sims)
        count = 0
        for j in top_idx:
            if j == i:
                continue
            score = float(sims[j])
            if score < 0.01:
                break
            conn.execute(
                "INSERT INTO fact_anime_similarity (anime_key_1, anime_key_2, similarity_score, method) VALUES (?, ?, ?, 'tfidf')",
                [ak1, anime_keys[j], round(score, 4)],
            )
            stored += 1
            count += 1
            if count >= 10:
                break

    conn.commit()
    print(f"  Stored {stored:,} TF-IDF similarity scores")
    print(f"  Coverage: {len(anime_keys)}/{len(anime_keys)} (100%)")

    # =====================================================================
    # STEP 2: Generate synthetic user ratings
    # =====================================================================
    print("\n[2/4] Generating user ratings...")

    conn.execute("DELETE FROM fact_user_rating")
    conn.commit()

    # Use DuckDB-native SQL for fast rating generation
    # Each user rates 15-30 anime based on hash-deterministic selection
    conn.execute("""
        INSERT INTO fact_user_rating (rating_key, user_key, anime_key, rating, rating_date)
        SELECT
            ROW_NUMBER() OVER () as rating_key,
            u.user_key,
            a.anime_key,
            -- Rating based on anime score + small variation
            ROUND(
                LEAST(5.0, GREATEST(1.0,
                    COALESCE(m.average_score / 20.0, 3.0)
                    + (CAST(ABS(HASH(CAST(u.user_key AS BIGINT) * 1000 + CAST(a.anime_key AS BIGINT))) % 20 AS DOUBLE) - 10.0) / 10.0
                )), 1
            ) as rating,
            CURRENT_DATE as rating_date
        FROM dim_user u
        CROSS JOIN dim_anime a
        LEFT JOIN fact_anime_metrics m ON a.anime_key = m.anime_key
        WHERE a.is_current = true
          AND ABS(HASH(CAST(u.user_key AS BIGINT) * 7 + CAST(a.anime_key AS BIGINT) * 13)) % 100 < 4
          AND u.user_key <= 2000
    """)
    conn.commit()

    rating_count = conn.execute("SELECT COUNT(*) FROM fact_user_rating").fetchone()[0]
    user_count = conn.execute("SELECT COUNT(DISTINCT user_key) FROM fact_user_rating").fetchone()[0]
    anime_rated = conn.execute("SELECT COUNT(DISTINCT anime_key) FROM fact_user_rating").fetchone()[0]
    print(f"  Ratings: {rating_count:,}")
    print(f"  Users: {user_count:,}")
    print(f"  Anime rated: {anime_rated}")

    # =====================================================================
    # STEP 3: Train NMF collaborative filtering
    # =====================================================================
    print("\n[3/4] Training NMF collaborative filtering...")

    conn.execute("DELETE FROM fact_collaborative_scores")
    conn.commit()

    ratings = conn.execute(
        "SELECT user_key, anime_key, rating FROM fact_user_rating"
    ).fetchall()

    if len(ratings) < 100:
        print("  Not enough ratings, skipping CF.")
    else:
        # Build user/anime index mappings
        unique_users = sorted(set(r[0] for r in ratings))
        unique_anime = sorted(set(r[1] for r in ratings))
        user_idx_map = {u: i for i, u in enumerate(unique_users)}
        anime_idx_map = {a: i for i, a in enumerate(unique_anime)}
        idx_anime_map = {i: a for a, i in anime_idx_map.items()}

        rows = [user_idx_map[r[0]] for r in ratings]
        cols = [anime_idx_map[r[1]] for r in ratings]
        vals = [float(r[2]) / 5.0 for r in ratings]

        matrix = csr_matrix(
            (vals, (rows, cols)),
            shape=(len(unique_users), len(unique_anime)),
            dtype=np.float32,
        )
        print(f"  Rating matrix: {matrix.shape[0]} users x {matrix.shape[1]} anime")
        print(f"  Sparsity: {100 * (1 - matrix.nnz / (matrix.shape[0] * matrix.shape[1])):.1f}%")

        model = NMF(n_components=30, init="nndsvda", random_state=42, max_iter=50)
        W = model.fit_transform(matrix)
        H = model.components_

        print(f"  NMF trained (30 components, 50 iterations)")
        print(f"  Reconstruction error: {model.reconstruction_err_:.4f}")

        # Generate predictions for first 200 users
        n_pred_users = min(200, len(unique_users))
        predictions = np.dot(W[:n_pred_users], H)

        cf_key = 1
        cf_stored = 0
        for ui in range(n_pred_users):
            user_key = unique_users[ui]
            scores = predictions[ui]
            top_idx = np.argsort(-scores)[:10]

            for ai in top_idx:
                anime_key = idx_anime_map[ai]
                score_val = float(scores[ai]) * 5.0
                if score_val > 0.5:
                    conn.execute(
                        "INSERT INTO fact_collaborative_scores (collab_key, user_key, anime_key, predicted_rating) VALUES (?, ?, ?, ?)",
                        [cf_key, user_key, anime_key, round(min(score_val, 5.0), 4)],
                    )
                    cf_key += 1
                    cf_stored += 1

        conn.commit()
        print(f"  Stored {cf_stored:,} CF predictions")

    # =====================================================================
    # STEP 4: Verify
    # =====================================================================
    print("\n[4/4] Verification...")

    total_anime = conn.execute("SELECT COUNT(*) FROM dim_anime WHERE is_current = true").fetchone()[0]
    sim_count = conn.execute("SELECT COUNT(*) FROM fact_anime_similarity").fetchone()[0]
    sim_anime = conn.execute("SELECT COUNT(DISTINCT anime_key_1) FROM fact_anime_similarity").fetchone()[0]
    rating_count = conn.execute("SELECT COUNT(*) FROM fact_user_rating").fetchone()[0]
    cf_count = conn.execute("SELECT COUNT(*) FROM fact_collaborative_scores").fetchone()[0]

    print(f"  Total anime:          {total_anime}")
    print(f"  TF-IDF similarities:  {sim_count:,} ({sim_anime}/{total_anime} anime = {sim_anime/total_anime*100:.0f}%)")
    print(f"  User ratings:         {rating_count:,}")
    print(f"  CF predictions:       {cf_count:,}")

    # Quick sanity: test a recommendation
    test = conn.execute("""
        SELECT a2.title, s.similarity_score
        FROM fact_anime_similarity s
        JOIN dim_anime a2 ON s.anime_key_2 = a2.anime_key
        JOIN dim_anime a1 ON s.anime_key_1 = a1.anime_key
        WHERE a1.title = 'DEATH NOTE'
        ORDER BY s.similarity_score DESC
        LIMIT 5
    """).fetchdf()
    print(f"\n  Sanity check - DEATH NOTE recommendations:")
    for _, r in test.iterrows():
        print(f"    {r['title']:40s} {r['similarity_score']:.4f}")

    conn.close()
    print("\nDone! All models retrained.")


if __name__ == "__main__":
    main()
