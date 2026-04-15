from __future__ import annotations

import logging

from rich.progress import SpinnerColumn

logger = logging.getLogger(__name__)


spinner = SpinnerColumn(style="green", spinner_name="dots")
