from dagster import Definitions, load_assets_from_modules

from .assets import export, letterboxd, metrics, opensubtitles
from .jobs import full_reload_job
from .resources import database_resource

letterboxd_assets = load_assets_from_modules([letterboxd])
opensubtitles_assets = load_assets_from_modules([opensubtitles])
metric_assets = load_assets_from_modules([metrics])
export_assets = load_assets_from_modules([export])

defs = Definitions(
    assets=[*letterboxd_assets, *opensubtitles_assets, *metric_assets, *export_assets],
    resources={"database": database_resource},
    jobs=[full_reload_job],
)
