from dagster import Definitions, load_assets_from_modules

from .assets import letterboxd, opensubtitles
from .resources import database_resource

letterboxd_assets = load_assets_from_modules([letterboxd])
opensubtitles_assets = load_assets_from_modules([opensubtitles])

defs = Definitions(
    assets=[*letterboxd_assets, *opensubtitles_assets],
    resources={"database": database_resource},
)
