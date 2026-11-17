"""Check local Markdown links and images without network access."""

import subprocess
from pathlib import Path
from urllib.parse import unquote, urlsplit

from markdown_it import MarkdownIt


ROOT = Path(__file__).resolve().parents[1]


def missing_local_links(document: Path, text: str) -> list[str]:
    missing = []
    for token in MarkdownIt().parse(text):
        for child in token.children or []:
            if child.type not in ("link_open", "image"):
                continue
            destination = child.attrGet("href" if child.type == "link_open" else "src")
            if not destination:
                continue
            url = urlsplit(destination)
            if url.scheme or url.netloc or not url.path or url.path.startswith("/"):
                continue
            if not (document.parent / unquote(url.path)).exists():
                line = token.map[0] + 1 if token.map else 1
                missing.append(f"{document}:{line}: {destination}")
    return missing


def test_local_markdown_links():
    result = subprocess.run(
        ["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard",
         "--", "*.md", "*.mdx"],
        cwd=ROOT, check=True, stdout=subprocess.PIPE,
    )
    documents = [ROOT / path for path in result.stdout.decode("utf-8").split("\0") if path]
    assert documents, "No Markdown files found"
    missing = []
    for document in documents:
        missing.extend(missing_local_links(document, document.read_text(encoding="utf-8-sig")))
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


def test_external_links_fragments_and_code_are_not_checked(tmp_path):
    text = (
        "[external](https://example.com/missing.md) [section](#heading)\n\n"
        "`[inline code](missing.md)`\n\n"
        "```md\n[example](missing.md)\n```\n"
    )
    assert missing_local_links(tmp_path / "guide.md", text) == []
