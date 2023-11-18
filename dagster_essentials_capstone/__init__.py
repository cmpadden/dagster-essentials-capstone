from dagster import Definitions, load_assets_from_modules

from . import assets
from .resources import database_resource

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    resources={"database": database_resource},
)
