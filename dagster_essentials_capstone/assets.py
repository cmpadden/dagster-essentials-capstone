""" Letterboxd assets

The Letterboxd API (https://letterboxd.com/api-beta/) is currently in private beta,
therefore, we will have to scrape the website the ol' fashioned way.

"""
import pandas as pd
import requests
from dagster import asset
from dagster_duckdb import DuckDBResource
from lxml import html

# The landing page for popular films on Letterbox (letterboxd.com/films/popular/) is
# dynamically loaded. By inspecting the network tab, we can see that the content is
# loaded from an `/ajax/` route; we can hit this directly to get the list of films.
LETTERBOXD_POPULAR_FILMS_URL = (
    "https://letterboxd.com/films/ajax/popular/?esiAllowFilters=true"
)

LETTERBOXD_REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; rv:109.0) Gecko/20100101 Firefox/117.0",
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.5",
    "Referer": "https://letterboxd.com/films/popular/",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "X-Requested-With": "XMLHttpRequest",
}


@asset
def letterboxd_popular_films(database: DuckDBResource):
    """First `n` pages of popular films scraped from Letterboxd."""

    response = requests.get(
        LETTERBOXD_POPULAR_FILMS_URL, headers=LETTERBOXD_REQUEST_HEADERS
    )

    assert response.status_code == 200

    tree = html.fromstring(response.content)

    # Example film poster element:
    #
    #     <li class="listitem poster-container" data-average-rating="4.57">
    #       <div
    #         class="really-lazy-load poster film-poster film-poster-426406 linked-film-poster"
    #         data-image-width="70"
    #         data-image-height="105"
    #         data-film-id="426406"
    #         data-film-slug="parasite-2019"
    #         data-poster-url="/film/parasite-2019/image-150/"
    #         data-linked="linked"
    #         data-target-link="/film/parasite-2019/"
    #         data-target-link-target=""
    #         data-cache-busting-key="0968db69"
    #         data-show-menu="true"
    #       >
    #         <img
    #           src="https://s.ltrbxd.com/static/img/empty-poster-70.8112b435.png"
    #           class="image"
    #           width="70"
    #           height="105"
    #           alt="Parasite"
    #         />
    #         <span class="frame">
    #           <span class="frame-title">
    #           </span>
    #         </span>
    #       </div>
    #     </li>
    #
    # Extract each each `data-*` attribute along with the image source.

    # fmt: off
    name_sub_selector_pairs: list[tuple[str, str]] = [
        ("average_rating",   "/@data-average-rating"),
        ("film_id",          "/div/@data-film-id"),
        ("film_slug",        "/div/@data-film-slug"),
        ("poster_url",       "/div/@data-poster-url"),
        ("link",             "/div/@data-target-link"),
        ("poster_image_url", "/div/img/@src"),
    ]
    # fmt: on

    # Extracts sub-selector content from `tree` using defined xpaths, and construct a
    # Pandas dataframe with column names defined in `name_sub_selector_pairs` mapping.
    df = pd.DataFrame(
        {
            name: tree.xpath(f"//li[contains(@class, 'listitem')]/{sub_selector}")
            for name, sub_selector in name_sub_selector_pairs
        }
    )

    df["snapshot_ts"] = pd.Timestamp.now()

    with database.get_connection() as conn:
        df.to_sql("letterboxd_popular_films", conn, index=False, if_exists="append")
