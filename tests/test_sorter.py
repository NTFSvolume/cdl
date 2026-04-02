import shutil
from pathlib import Path

import pytest

from cyberdrop_dl.utils.sorting import _have_same_content, _move_file


class TestHaveSameContent:
    def test_same_file_returns_true(self, tmp_path: Path) -> None:
        foo = tmp_path / "foo.bin"
        foo.write_bytes(b"12345")
        assert _have_same_content(foo, foo)

    def test_same_content_same_size_returns_true(self, tmp_path: Path) -> None:
        foo, bar = tmp_path / "foo.bin", tmp_path / "bar.bin"
        data = b"0" * 1_000
        foo.write_bytes(data)
        bar.write_bytes(data)
        assert _have_same_content(foo, bar)

    def test_different_size_returns_false(self, tmp_path: Path) -> None:
        foo, bar = tmp_path / "foo.bin", tmp_path / "bar.bin"
        foo.write_bytes(b"x")
        bar.write_bytes(b"x" * 10)
        assert not _have_same_content(foo, bar)

    def test_same_size_different_content_returns_false(self, tmp_path: Path) -> None:
        foo, bar = tmp_path / "foo.bin", tmp_path / "bar.bin"
        foo.write_bytes(b"\x00" * 512)
        bar.write_bytes(b"\xff" * 512)
        assert not _have_same_content(foo, bar)

    def test_empty_files_return_true(self, tmp_path: Path) -> None:
        foo, bar = tmp_path / "foo.bin", tmp_path / "bar.bin"
        foo.touch()
        bar.touch()
        assert _have_same_content(foo, bar)

    def test_large_files_same_content_return_true(self, tmp_path: Path) -> None:
        foo, bar = tmp_path / "foo.bin", tmp_path / "bar.bin"
        data_10MB = b"A" * 1024 * 1024 * 10
        foo.write_bytes(data_10MB)
        bar.write_bytes(data_10MB)
        assert _have_same_content(foo, bar)


class TestMoveFile:
    def test_same_file_returns_immediately(self, tmp_path: Path) -> None:
        foo = tmp_path / "foo.txt"
        foo.write_text("x")
        assert _move_file(foo, foo) == foo
        assert foo.exists()

    def test_simple_move(self, tmp_path: Path) -> None:
        src = tmp_path / "foo.txt"
        src.write_text("data")
        dst = tmp_path / "sub" / "bar.txt"
        out = _move_file(src, dst)
        assert out == dst
        assert not src.exists()
        assert dst.read_text() == "data"

    def test_name_collision_same_size_diferent_content(self, tmp_path: Path) -> None:
        src = tmp_path / "foo.txt"
        src.write_text("abc")
        dst = tmp_path / "bar.txt"
        dst.write_text("123")
        out = _move_file(src, dst)

        assert out
        assert out != dst
        assert not src.exists()
        assert dst.read_text() == "123"
        assert out.read_text() == "abc"

    def test_name_collision_different_size(self, tmp_path: Path) -> None:
        src = tmp_path / "foo.jpg"
        src.write_bytes(b"0" * 100)
        dst = tmp_path / "bar.jpg"
        dst.write_bytes(b"1" * 50)
        out = _move_file(src, dst)
        assert out
        assert out == tmp_path / "bar1.jpg"
        assert not src.exists()
        assert out.stat().st_size == 100

    def test_custom_incrementer_format(self, tmp_path: Path) -> None:
        src = tmp_path / "foo.log"
        src.write_text("a")
        dest = tmp_path / "bar.log"
        dest.write_text("x")
        for i in range(1, 4):
            (tmp_path / f"bar_{i}.log").write_text("x")
        out = _move_file(src, dest, incrementer_format="_{i}")
        assert out == tmp_path / "bar_4.log"

    def test_max_retries_exceeded(self, tmp_path: Path) -> None:
        src = tmp_path / "foo.txt"
        src.write_text("x")
        dest = tmp_path / "bar.txt"
        dest.touch()
        for i in range(1, 12):
            (tmp_path / f"bar{i}.txt").touch()

        assert _move_file(src, dest) is None
        assert src.exists()

    def test_os_error_during_move_cancels_it(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        src = tmp_path / "foo.txt"
        src.write_text("x")
        dst = tmp_path / "bar.txt"

        def boom(*_, **_k):
            raise OSError

        monkeypatch.setattr(shutil, "move", boom)
        assert _move_file(src, dst) is None
        assert src.exists()
