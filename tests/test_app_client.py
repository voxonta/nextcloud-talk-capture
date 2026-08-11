"""The address the interceptor asks for calls at."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from talk_capture.app_client import AppClient  # noqa: E402
from talk_capture.config import CaptureConfig  # noqa: E402


def test_default_app_id_is_the_installed_one():
    # The app was renamed to `voxonta`; asking for the old id answered 404 and
    # the interceptor saw no calls at all for a week.
    client = AppClient("https://cloud.example", "t")
    assert client._base == "https://cloud.example/apps/voxonta/api/v1/service"


def test_app_id_follows_a_rename_without_a_rebuild():
    client = AppClient("https://cloud.example", "t", app_id="something_else")
    assert client._base == "https://cloud.example/apps/something_else/api/v1/service"


def test_trailing_slash_does_not_double_up():
    client = AppClient("https://cloud.example/", "t")
    assert client._base == "https://cloud.example/apps/voxonta/api/v1/service"


def test_config_reads_the_app_id_from_the_environment(monkeypatch):
    monkeypatch.setenv("NEXTCLOUD_URL", "https://cloud.example")
    monkeypatch.setenv("NC_APP_ID", "renamed")
    assert CaptureConfig.from_env().app_id == "renamed"


def test_config_defaults_to_the_installed_app_id(monkeypatch):
    monkeypatch.setenv("NEXTCLOUD_URL", "https://cloud.example")
    monkeypatch.delenv("NC_APP_ID", raising=False)
    assert CaptureConfig.from_env().app_id == "voxonta"
