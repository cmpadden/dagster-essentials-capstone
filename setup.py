from setuptools import find_packages, setup

setup(
    name="dagster_essentials_capstone",
    packages=find_packages(exclude=["dagster_essentials_capstone_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "dagster-duckdb",
        "requests",
        "pandas",
    ],
    extras_require={"dev": ["dagster-webserver", "pytest", "ruff", "isort"]},
)
