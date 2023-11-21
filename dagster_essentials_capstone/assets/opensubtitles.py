""" OpenSubtitles
"""
import os
import re
import zipfile
from urllib import parse

import requests
from dagster import asset
from dagster_duckdb import DuckDBResource
from lxml import html

from ..constants import DUCKDB_TABLE_LETTERBOXD_FILMS_DETAILS

OPENSUBTITLES_SEARCH_URL_BASE = "https://www.opensubtitles.org/en/search/imdbid-6751668/sublanguageid-eng/moviename-"


@asset(deps=["letterboxd_film_details"])
def film_open_subtitles_raw(database: DuckDBResource):

    with database.get_connection() as conn:
        results = conn.execute(
            f"""
            select
                film_slug,
                external_links
            from {DUCKDB_TABLE_LETTERBOXD_FILMS_DETAILS}
            where snapshot_ts = (
                select max(snapshot_ts) from {DUCKDB_TABLE_LETTERBOXD_FILMS_DETAILS}
            )
            """
        ).fetchall()

    for film_slug, external_links in results:
        zip_file_target = f"data/raw/opensubtitles/{film_slug}.zip"
        zip_decompressed_target = f"data/staging/opensubtitles/{film_slug}"

        # skip downloading subtitles that exist on the filesystem
        if os.path.exists(zip_file_target) and os.path.exists(zip_decompressed_target):
            continue

        # It is possible to search opensubtitles using the IMDB ID
        imdb_id = re.findall(r"\/tt(\d+)\/", external_links)

        url = f"https://www.opensubtitles.org/en/search/imdbid-{imdb_id}/sublanguageid-eng"

        response = requests.get(url)

        assert response.status_code == 200

        tree = html.fromstring(response.content)

        table_hrefs = tree.xpath("//table[@id='search_results']/tbody/tr/td/a/@href")

        tree.xpath("//table[@id='search_results']")

        subtitle_hrefs = [h for h in table_hrefs if "/subtitleserve/" in h]

        assert len(subtitle_hrefs) > 0

        target_subtitle_href = subtitle_hrefs[0]

        response = requests.get(f"https://www.opensubtitles.org{target_subtitle_href}")

        with open(zip_file_target, "wb") as f:
            f.write(response.content)

        with zipfile.ZipFile(zip_file_target, "r") as f:
            f.extractall(zip_decompressed_target)
