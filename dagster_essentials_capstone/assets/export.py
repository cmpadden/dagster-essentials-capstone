""" Exports used in website.
"""
from dagster import asset
from dagster_duckdb import DuckDBResource

from dagster_essentials_capstone.constants import (
    DUCKDB_TABLE_LETTERBOXD_FILMS_DETAILS, DUCKDB_TABLE_OPENAI_SUMMARY)


@asset(deps=["openai_film_summary", "letterboxd_poster_image"])
def film_details_json_dump(database: DuckDBResource):
    """Exported JSON data for use in website.
    """
    with database.get_connection() as conn:
        conn.execute(
            f"""
            copy (
                select
                  d.film_slug,
                  d.title,
                  d.synopsis,
                  d.description,
                  d.release_date,
                  d.external_links,
                  d.genre_links,
                  d.genre_names,
                  d.cast_details,
                  d.stats,
                  s.openai_total_tokens,
                  s.openai_prompt_tokens,
                  s.openai_completion_tokens,
                  s.openai_total_cost,
                  s.summary
                from {DUCKDB_TABLE_LETTERBOXD_FILMS_DETAILS} as d
                left join {DUCKDB_TABLE_OPENAI_SUMMARY} as s
                  on d.film_slug = s.film_slug
            ) to 'data/production/film_details.json';
            """
        )
