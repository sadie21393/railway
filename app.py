import os
import sqlite3
from flask import Flask, jsonify
from flask_cors import CORS
import pyreadstat
import pandas as pd

app = Flask(__name__)
CORS(app)

# === Load recommender.sav ===
sav_file = os.path.join(os.path.dirname(__file__), "recommender.sav")
try:
    df, meta = pyreadstat.read_sav(sav_file)
    print("Raw DataFrame from recommender.sav:")
    print(df.head())

    recommendations_lookup = {}
    for user_id, user_df in df.groupby('user_id'):
        top_all = user_df[user_df['rec_type'] == 'top_all'][['show_id', 'title', 'match_score']].to_dict('records')
        top_genre = user_df[user_df['rec_type'] == 'top_genre'][['show_id', 'title', 'match_score']].to_dict('records')
        second_genre = user_df[user_df['rec_type'] == 'second_genre'][['show_id', 'title', 'match_score']].to_dict('records')

        top_genre_name = user_df[user_df['rec_type'] == 'top_genre']['genre_name'].iloc[0] if not user_df[user_df['rec_type'] == 'top_genre'].empty else 'None'
        second_genre_name = user_df[user_df['rec_type'] == 'second_genre']['genre_name'].iloc[0] if not user_df[user_df['rec_type'] == 'second_genre'].empty else 'None'

        recommendations_lookup[user_id] = {
            "top_all_recs": top_all,
            "top_genre_recs": top_genre,
            "second_genre_recs": second_genre,
            "top_genre_name": top_genre_name,
            "second_genre_name": second_genre_name
        }
    print("Loaded recommender.sav successfully")
except Exception as e:
    recommendations_lookup = {}
    print("Error loading sav file:", e)

# === Load content_recommendations.sav ===
content_sav_file = os.path.join(os.path.dirname(__file__), "content_recommendations.sav")
try:
    content_df, content_meta = pyreadstat.read_sav(content_sav_file)
    content_recommendations_lookup = {}
    for show_id, show_df in content_df.groupby('show_id'):
        content_recommendations_lookup[show_id] = show_df[['recommended_show_id', 'recommended_title']].to_dict('records')
    print("Loaded content_recommendations.sav successfully")
except Exception as e:
    content_recommendations_lookup = {}
    print("Error loading content sav file:", e)

# === Movie details from database ===
def get_movie_details_by_show_ids(show_ids):
    db_path = os.path.join(os.path.dirname(__file__), "Movies.db")
    db_path = os.path.abspath(db_path)
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    cursor = connection.cursor()
    placeholders = ','.join('?' for _ in show_ids)
    query = f"SELECT * FROM movies_titles WHERE show_id IN ({placeholders})"
    cursor.execute(query, show_ids)
    rows = cursor.fetchall()
    connection.close()
    return [dict(row) for row in rows]

def get_average_rating(show_id):
    db_path = os.path.join(os.path.dirname(__file__), "Movies.db")
    db_path = os.path.abspath(db_path)
    connection = sqlite3.connect(db_path)
    cursor = connection.cursor()
    query = "SELECT AVG(rating) as avg_rating FROM movies_ratings WHERE show_id = ?"
    cursor.execute(query, (show_id,))
    result = cursor.fetchone()
    connection.close()
    return result[0] if result[0] is not None else 3.5

# === Helper for movie transformation ===
def transform_movie(movie, match_score=0.0):
    show_id = movie.get("show_id")
    average_rating = get_average_rating(show_id)
    return {
        "movieId": show_id,
        "title": movie.get("title"),
        "description": movie.get("description", "No description available"),
        "duration": movie.get("duration", "N/A"),
        "rating": movie.get("rating", "N/A"),
        "year": movie.get("release_year", 0),
        "averageRating": round(average_rating, 1),
        "matchScore": match_score
    }

# === Main Recommendation Endpoint ===
@app.route("/api/recommendations/<user_id>", methods=["GET"])
def recommendations(user_id):
    user_id_str = str(user_id)
    recs = recommendations_lookup.get(user_id_str, {
        "top_all_recs": [],
        "top_genre_recs": [],
        "second_genre_recs": [],
        "top_genre_name": "None",
        "second_genre_name": "None"
    })

    all_show_ids = (
        [rec['show_id'] for rec in recs["top_all_recs"]] +
        [rec['show_id'] for rec in recs["top_genre_recs"]] +
        [rec['show_id'] for rec in recs["second_genre_recs"]]
    )

    if all_show_ids:
        raw_movies = get_movie_details_by_show_ids(all_show_ids)
        movies_dict = {movie['show_id']: movie for movie in raw_movies}
    else:
        movies_dict = {}

    top_all_movies = [
        transform_movie(movies_dict.get(rec['show_id'], {'show_id': rec['show_id'], 'title': rec['title']}), rec['match_score'])
        for rec in recs["top_all_recs"]
    ]
    top_genre_movies = [
        transform_movie(movies_dict.get(rec['show_id'], {'show_id': rec['show_id'], 'title': rec['title']}))
        for rec in recs["top_genre_recs"]
    ]
    second_genre_movies = [
        transform_movie(movies_dict.get(rec['show_id'], {'show_id': rec['show_id'], 'title': rec['title']}))
        for rec in recs["second_genre_recs"]
    ]

    return jsonify({
        "user_id": user_id_str,
        "recommendations": {
            "top_all": top_all_movies,
            "top_genre": top_genre_movies,
            "second_genre": second_genre_movies,
            "top_genre_name": recs["top_genre_name"],
            "second_genre_name": recs["second_genre_name"]
        }
    })

# === Content-Based Recommendation Endpoint ===
@app.route("/api/recommendations/content/<show_id>", methods=["GET"])
def content_recommendations(show_id):
    show_id_str = str(show_id)
    recs = content_recommendations_lookup.get(show_id_str, [])

    if not recs:
        return jsonify({
            "show_id": show_id_str,
            "recommendations": []
        })

    recommended_show_ids = [rec['recommended_show_id'] for rec in recs]

    if recommended_show_ids:
        raw_movies = get_movie_details_by_show_ids(recommended_show_ids)
        movies_dict = {movie['show_id']: movie for movie in raw_movies}
    else:
        movies_dict = {}

    recommended_movies = [
        transform_movie(movies_dict.get(rec['recommended_show_id'], {
            'show_id': rec['recommended_show_id'],
            'title': rec['recommended_title']
        }))
        for rec in recs
    ]

    return jsonify({
        "show_id": show_id_str,
        "recommendations": recommended_movies
    })

# === Run App for Railway ===
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    app.run(host="0.0.0.0", port=port)
