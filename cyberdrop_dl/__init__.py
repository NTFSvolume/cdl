import importlib.metadata
import sys

_ = sys.path.pop(0)


__version__ = importlib.metadata.version("cyberdrop-dl-patched")
