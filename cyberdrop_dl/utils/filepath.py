from __future__ import annotations

import mimetypes
import platform
import re
import unicodedata
from contextvars import ContextVar
from pathlib import Path

from cyberdrop_dl import constants
from cyberdrop_dl.exceptions import InvalidExtensionError, NoExtensionError

_ALLOWED_FILEPATH_PUNCTUATION = " .-_!#$%'()+,;=@[]^{}~"
_SANITIZE_FILENAME_PATTERN = r'[<>:"/\\|?*\']'
_RAR_MULTIPART_PATTERN = r"^part\d+"
MAX_FILE_LEN: ContextVar[int] = ContextVar("_MAX_FILE_LEN")
MAX_FOLDER_LEN: ContextVar[int] = ContextVar("_MAX_FOLDER_LEN")


def sanitize_unicode_emojis_and_symbols(title: str) -> str:
    """Allow all Unicode letters/numbers/marks, plus safe filename punctuation, but not symbols or emoji."""
    return "".join(
        c for c in title if (c in _ALLOWED_FILEPATH_PUNCTUATION or unicodedata.category(c)[0] in {"L", "N", "M"})
    ).strip()


def sanitize_filename(name: str, sub: str = "") -> str:
    clean_name = re.sub(_SANITIZE_FILENAME_PATTERN, sub, name)
    if platform.system() in ("Windows", "Darwin"):
        return sanitize_unicode_emojis_and_symbols(clean_name)
    return clean_name


def sanitize_folder(title: str, max_len: int | None = None) -> str:
    max_len = max_len or MAX_FOLDER_LEN.get()
    title = title.replace("\n", "").replace("\t", "").strip()
    title = sanitize_filename(re.sub(r" +", " ", title), "-")
    title = re.sub(r"\.{2,}", ".", title).rstrip(".").strip()

    if all(char in title for char in ("(", ")")):
        new_title, domain_part = title.rsplit("(", 1)
        new_title = truncate_str(new_title, max_len)
        return f"{new_title} ({domain_part.strip()}"

    return truncate_str(title, max_len)


def truncate_str(text: str, max_bytes: int) -> str:
    str_bytes = text.encode("utf-8")[:max_bytes]
    return str_bytes.decode("utf-8", "ignore")


def get_filename_and_ext(
    filename: str,
    mime_type: str | None = None,
    max_len: int | None = None,
    *,
    xenforo: bool = False,
) -> tuple[str, str]:
    filename_as_path = Path(Path(filename).as_posix().replace("/", "-"))  # remove OS separators
    if not filename_as_path.suffix:
        if mime_type and (ext := mimetypes.guess_extension(mime_type)):
            filename_as_path = filename_as_path.with_suffix(ext)
        else:
            raise NoExtensionError(filename)

    if xenforo and "-" in filename and filename_as_path.suffix.lstrip(".").isdigit():
        name, _, ext = filename_as_path.name.rpartition("-")
        ext = ext.rsplit(".")[0]
        filename = f"{name}.{ext}"
        if ext.lower() not in constants.FileExt.MEDIA:
            raise InvalidExtensionError(filename)

        filename_as_path = Path(filename)

    return _get_filename_and_ext(filename_as_path, max_len or MAX_FILE_LEN.get())


def _get_filename_and_ext(filename_as_path: Path, max_len: int) -> tuple[str, str]:
    if len(filename_as_path.suffix) > 5:
        raise InvalidExtensionError(str(filename_as_path))

    filename_as_path = filename_as_path.with_suffix(filename_as_path.suffix.lower())
    filename = truncate_str(filename_as_path.stem, max_len - len(filename_as_path.suffix)) + filename_as_path.suffix
    filename_as_path = Path(sanitize_filename(filename))
    return filename_as_path.stem.strip(), filename_as_path.suffix


def compose_custom_filename(
    stem: str,
    ext: str,
    max_len: int | None = None,
    *extras: str,
    only_truncate_stem: bool,
) -> tuple[str, bool]:
    max_len = max_len or MAX_FILE_LEN.get()
    truncate_len = max_len - len(ext)
    has_invalid_extra_info_chars = False
    if extras:
        extra = "".join(f"[{info}]" for info in extras)
        clean_extras = sanitize_filename(extra)
        has_invalid_extra_info_chars = clean_extras != extra
        if only_truncate_stem and (new_truncate_len := truncate_len - len(clean_extras) - 1) > 0:
            truncated_stem = f"{truncate_str(stem, new_truncate_len)} {clean_extras}"
        else:
            truncated_stem = truncate_str(f"{stem} {clean_extras}", truncate_len)

    else:
        truncated_stem = truncate_str(stem, truncate_len)

    return f"{truncated_stem}{ext}", has_invalid_extra_info_chars


def remove_file_id(filename: str, ext: str) -> str:
    """Removes the additional string some websites adds to the end of every filename."""

    filename = filename.rsplit(ext, 1)[0]
    filename = filename.rsplit("-", 1)[0]
    tail_no_dot = filename.rsplit("-", 1)[-1]
    ext_no_dot = ext.rsplit(".", 1)[-1]
    tail = f".{tail_no_dot}"
    if re.match(_RAR_MULTIPART_PATTERN, tail_no_dot) and ext == ".rar" and "-" in filename:
        filename, part = filename.rsplit("-", 1)
        filename = f"{filename}.{part}"
    elif ext_no_dot.isdigit() and tail in constants.FileExt.SEVEN_Z and "-" in filename:
        filename, _7z_ext = filename.rsplit("-", 1)
        filename = f"{filename}.{_7z_ext}"
    if not filename.endswith(ext):
        filename = filename + ext
    return filename
