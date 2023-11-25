from dagster import AssetSelection, define_asset_job

full_reload_assets = AssetSelection.keys(
    "film_details_json_dump",
    "film_open_subtitles_raw",
    "letterboxd_film_details",
    "letterboxd_popular_films",
    "letterboxd_poster_image",
    "openai_film_summary",
)


full_reload_job = define_asset_job(
    name="full_reload_job",
    selection=full_reload_assets,
)
