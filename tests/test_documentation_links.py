"""Check local Markdown links and images without network access."""

import subprocess
from pathlib import Path
from urllib.parse import unquote, urlsplit

from markdown_it import MarkdownIt
import pytest


ROOT = Path(__file__).resolve().parents[1]


def missing_local_links(
    document: Path, text: str, *, available_paths: set[Path] | None = None,
) -> list[str]:
    missing = []
    for token in MarkdownIt().parse(text):
        for child in token.children or []:
            if child.type not in ("link_open", "image"):
                continue
            destination = child.attrGet("href" if child.type == "link_open" else "src")
            if not destination:
                continue
            assert isinstance(destination, str), "Markdown link targets must be strings"
            url = urlsplit(destination)
            if url.scheme or url.netloc or not url.path or url.path.startswith("/"):
                continue
            target = (document.parent / unquote(url.path)).resolve()
            if not target.exists() or (
                available_paths is not None and target not in available_paths
            ):
                line = token.map[0] + 1 if token.map else 1
                missing.append(f"{document}:{line}: {destination}")
    return missing


def test_local_markdown_links():
    result = subprocess.run(
        ["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"],
        cwd=ROOT, check=True, stdout=subprocess.PIPE,
    )
    files = [(ROOT / path).resolve() for path in result.stdout.decode("utf-8").split("\0") if path]
    documents = [path for path in files if path.suffix.lower() in {".md", ".mdx"}]
    assert documents, "No Markdown files found"
    available_paths = set(files)
    for path in files:
        for parent in path.parents:
            if parent == ROOT:
                available_paths.add(parent)
                break
            if not parent.is_relative_to(ROOT):
                break
            available_paths.add(parent)
    missing = []
    for document in documents:
        missing.extend(missing_local_links(
            document, document.read_text(encoding="utf-8-sig"),
            available_paths=available_paths,
        ))
    assert not missing, "Missing local Markdown links:\n" + "\n".join(missing)


def test_local_links_include_references_and_images(tmp_path):
    (tmp_path / "present file.md").write_text("", encoding="utf-8")
    text = (
        "[good](present%20file.md#section) [missing][ref]\n\n"
        "![image](missing.png)\n\n[ref]: missing.md\n"
    )
    missing = missing_local_links(tmp_path / "guide.md", text)
    assert len(missing) == 2
    assert any("missing.md" in item for item in missing)
    assert any("missing.png" in item for item in missing)


def test_link_check_rejects_ignored_local_targets(tmp_path, monkeypatch):
    subprocess.run(["git", "init", "-q"], cwd=tmp_path, check=True)
    (tmp_path / ".gitignore").write_text("private/\n", encoding="utf-8")
    (tmp_path / "guide.md").write_text(
        "[available](public/file.md) [unpublished](private/file.md)\n",
        encoding="utf-8",
    )
    for folder in ("public", "private"):
        (tmp_path / folder).mkdir()
        (tmp_path / folder / "file.md").write_text("", encoding="utf-8")
    monkeypatch.setitem(globals(), "ROOT", tmp_path)

    with pytest.raises(AssertionError, match="private/file.md"):
        test_local_markdown_links()


def test_external_links_fragments_and_code_are_not_checked(tmp_path):
    text = (
        "[external](https://example.com/missing.md) [section](#heading)\n\n"
        "`[inline code](missing.md)`\n\n"
        "```md\n[example](missing.md)\n```\n"
    )
    assert missing_local_links(tmp_path / "guide.md", text) == []
