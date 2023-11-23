from dagster import Definitions, load_assets_from_modules

from .assets import letterboxd, metrics, opensubtitles
from .resources import database_resource

letterboxd_assets = load_assets_from_modules([letterboxd])
opensubtitles_assets = load_assets_from_modules([opensubtitles])
metric_assets = load_assets_from_modules([metrics])

defs = Definitions(
    assets=[*letterboxd_assets, *opensubtitles_assets, *metric_assets],
    resources={"database": database_resource},
)
