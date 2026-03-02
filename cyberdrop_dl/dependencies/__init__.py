from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import apprise

else:
    try:
        import apprise
    except ImportError:
        apprise = None

__all__ = ["apprise"]
