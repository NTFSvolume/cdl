import logging
from typing import TYPE_CHECKING

logger = logging.getLogger(__name__)
if TYPE_CHECKING:
    import apprise

else:
    try:
        import apprise
    except ImportError:
        apprise = None

__all__ = ["apprise"]
