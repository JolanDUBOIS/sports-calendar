from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("sports-calendar")
except PackageNotFoundError:
    __version__ = "0.0.0-dev" # Fallback version if the package is not installed
