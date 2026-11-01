"""Check the installed wheel, without importing the source checkout."""
from importlib import metadata, resources
from pathlib import Path
import sys
import tomllib

import mailru_photos


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    with (root / "pyproject.toml").open("rb") as stream:
        project = tomllib.load(stream)["project"]
    expected = project["version"]
    expected_waitress = next(item.split("==", 1)[1] for item in project["optional-dependencies"]["web"]
                            if item.startswith("waitress=="))
    installed = Path(mailru_photos.__file__).resolve()
    if installed.is_relative_to(root):
        raise SystemExit("Expected the installed wheel, but imported the source checkout.")
    assert mailru_photos.__version__ == expected, "Package __version__ differs from pyproject.toml"
    assert metadata.version("mailru-photo-client") == expected, "Installed wheel version is incorrect"
    assert metadata.version("waitress") == expected_waitress, "Web dependency pins differ from the installed version"
    static = resources.files("mailru_photos").joinpath("web_static")
    for filename in ("index.html", "app.js", "style.css"):
        assert static.joinpath(filename).read_bytes(), f"Missing packaged static asset: {filename}"
    print(f"Verified installed mailru-photo-client {expected} on Python {sys.version.split()[0]}")


if __name__ == "__main__":
    main()
