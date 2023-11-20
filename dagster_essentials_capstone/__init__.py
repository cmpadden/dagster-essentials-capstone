from dagster import Definitions, load_assets_from_modules

from .assets import letterboxd
from .resources import database_resource

letterboxd_assets = load_assets_from_modules([letterboxd])

defs = Definitions(
    assets=[*letterboxd_assets],
    resources={"database": database_resource},
)
