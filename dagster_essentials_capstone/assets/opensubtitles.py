""" OpenSubtitles
"""
import os
import re
import zipfile

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

        # It is possible to search opensubtitles using the IMDB ID extracted from the
        # Letterboxd external links URL.
        imdb_id = re.findall(r"\/tt(\d+)\/", external_links)

        response = requests.get(
            f"https://www.opensubtitles.org/en/search/imdbid-{imdb_id}/sublanguageid-eng"
        )

        assert response.status_code == 200

        tree = html.fromstring(response.content)

        table_hrefs = tree.xpath("//table[@id='search_results']/tbody/tr/td/a/@href")

        subtitle_hrefs = [h for h in table_hrefs if "/subtitleserve/" in h]

        assert len(subtitle_hrefs) > 0

        # On occasion, the opensubtitle srt download will _not_ be a zip file, resulting
        # in the error:
        #
        #     zipfile.BadZipFile
        #
        # This is likely due to some form of throttling, or request error, as the result
        # is instead HTML. Need to make this more robust.
        #
        # The first "potential" fix was adding `allow_redirects=True`, but validation
        # still needs to be done for multi-part ZIP files.

        response = requests.get(
            f"https://www.opensubtitles.org{subtitle_hrefs[0]}", allow_redirects=True
        )

        with open(zip_file_target, "wb") as f:
            f.write(response.content)

        with zipfile.ZipFile(zip_file_target, "r") as f:
            f.extractall(zip_decompressed_target)
