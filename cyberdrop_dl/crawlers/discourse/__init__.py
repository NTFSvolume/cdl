# All Discourse websites function exactly the same
# We can create subclasses dynamically by their URL


from cyberdrop_dl.crawlers import create_crawlers

from ._discourse import DiscourseCrawler

_DISCOURSES_SITES = ["https://forums.plex.tv"]


DISCOURSE_CRAWLERS: set[type[DiscourseCrawler]] = create_crawlers(DiscourseCrawler, *_DISCOURSES_SITES)

__all__ = ["DISCOURSE_CRAWLERS", "DiscourseCrawler"]
