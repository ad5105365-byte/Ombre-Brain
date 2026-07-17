# ============================================================
# Test: photo gallery API pagination + thumbnails
# 相册接口测试：GET /api/images 分页 + 缩略图签名 URL（可选）
# ============================================================

import json
import pytest
from unittest.mock import patch

import server
import image_store


class FakeRequest:
    """Minimal stand-in for a starlette Request — query_params only, no body."""

    def __init__(self, query_params=None):
        self.query_params = query_params or {}


def _json(resp):
    return json.loads(resp.body)


async def _make_photo(bucket_mgr, n, created):
    content = (
        f"## 照片 {n}\n\n第{n}张\n\n"
        f"![photo](https://xxx.supabase.co/storage/v1/object/public/photos/p{n}.jpg)"
    )
    return await bucket_mgr.create(
        content=content, domain=["照片"], tags=["照片"],
        name=f"照片第{n}张", created=created)


# --- _parse_page_params ---

def test_parse_page_params_defaults():
    limit, offset = server._parse_page_params(FakeRequest({}), 200, 200)
    assert (limit, offset) == (200, 0)


def test_parse_page_params_defaults_match_server_constants():
    # dashboard.html 现有相册页不传 limit/offset，指望拿到全部照片——
    # 默认必须跟上限一致，否则会悄悄截断她相册里的老照片（见 server.py
    # IMG_PAGE_DEFAULT_LIMIT 旁边的注释）。
    assert server.IMG_PAGE_DEFAULT_LIMIT == server.IMG_PAGE_MAX_LIMIT


def test_parse_page_params_clamps_to_max():
    limit, offset = server._parse_page_params(FakeRequest({"limit": "9999"}), 30, 200)
    assert limit == 200


def test_parse_page_params_clamps_negative_offset():
    limit, offset = server._parse_page_params(FakeRequest({"offset": "-5"}), 30, 200)
    assert offset == 0


def test_parse_page_params_bad_values_fall_back():
    limit, offset = server._parse_page_params(
        FakeRequest({"limit": "not-a-number", "offset": "nope"}), 30, 200)
    assert (limit, offset) == (30, 0)


# --- GET /api/images pagination ---

@pytest.mark.asyncio
async def test_api_images_list_paginates_newest_first(bucket_mgr):
    await _make_photo(bucket_mgr, 1, "2026-07-01T00:00:00")
    await _make_photo(bucket_mgr, 2, "2026-07-02T00:00:00")
    await _make_photo(bucket_mgr, 3, "2026-07-03T00:00:00")

    with patch.object(server, "_require_auth", return_value=None), \
         patch.object(server, "bucket_mgr", bucket_mgr):
        resp = await server.api_images_list(FakeRequest({"limit": "2", "offset": "0"}))
    body = _json(resp)
    assert body["total"] == 3
    assert body["limit"] == 2 and body["offset"] == 0
    assert [p["name"] for p in body["photos"]] == ["照片第3张", "照片第2张"]


@pytest.mark.asyncio
async def test_api_images_list_second_page(bucket_mgr):
    await _make_photo(bucket_mgr, 1, "2026-07-01T00:00:00")
    await _make_photo(bucket_mgr, 2, "2026-07-02T00:00:00")
    await _make_photo(bucket_mgr, 3, "2026-07-03T00:00:00")

    with patch.object(server, "_require_auth", return_value=None), \
         patch.object(server, "bucket_mgr", bucket_mgr):
        resp = await server.api_images_list(FakeRequest({"limit": "2", "offset": "2"}))
    body = _json(resp)
    assert body["total"] == 3
    assert [p["name"] for p in body["photos"]] == ["照片第1张"]


@pytest.mark.asyncio
async def test_api_images_list_only_signs_current_page(bucket_mgr):
    await _make_photo(bucket_mgr, 1, "2026-07-01T00:00:00")
    await _make_photo(bucket_mgr, 2, "2026-07-02T00:00:00")
    await _make_photo(bucket_mgr, 3, "2026-07-03T00:00:00")

    seen_paths = []

    async def fake_sign_urls(paths, expires_in=3600, transform=None):
        seen_paths.append(list(paths))
        return {p: f"https://signed.example/{p}" for p in paths}

    with patch.object(server, "_require_auth", return_value=None), \
         patch.object(server, "bucket_mgr", bucket_mgr), \
         patch.object(server, "_img_is_configured", return_value=True), \
         patch("image_store.create_signed_urls", side_effect=fake_sign_urls):
        await server.api_images_list(FakeRequest({"limit": "1", "offset": "0"}))

    # 只签当页 1 张的 URL，不是相册里全部 3 张
    assert len(seen_paths) == 1
    assert len(seen_paths[0]) == 1
    assert seen_paths[0][0] == "p3.jpg"


@pytest.mark.asyncio
async def test_api_images_list_no_thumb_field_by_default(bucket_mgr):
    await _make_photo(bucket_mgr, 1, "2026-07-01T00:00:00")

    with patch.object(server, "_require_auth", return_value=None), \
         patch.object(server, "bucket_mgr", bucket_mgr), \
         patch.object(server, "_img_is_configured", return_value=True), \
         patch("image_store.create_signed_urls", side_effect=lambda paths, **kw:
               {p: f"https://signed.example/{p}" for p in paths}):
        resp = await server.api_images_list(FakeRequest({}))
    body = _json(resp)
    assert "thumb_url" not in body["photos"][0]


@pytest.mark.asyncio
async def test_api_images_list_thumbs_opt_in_requests_transform(bucket_mgr):
    await _make_photo(bucket_mgr, 1, "2026-07-01T00:00:00")
    calls = []

    async def fake_sign_urls(paths, expires_in=3600, transform=None):
        calls.append(transform)
        suffix = "-thumb" if transform else ""
        return {p: f"https://signed.example/{p}{suffix}" for p in paths}

    with patch.object(server, "_require_auth", return_value=None), \
         patch.object(server, "bucket_mgr", bucket_mgr), \
         patch.object(server, "_img_is_configured", return_value=True), \
         patch("image_store.create_signed_urls", side_effect=fake_sign_urls):
        resp = await server.api_images_list(FakeRequest({"thumbs": "1"}))
    body = _json(resp)
    photo = body["photos"][0]
    assert photo["thumb_url"].endswith("-thumb")
    assert photo["image_url"] and not photo["image_url"].endswith("-thumb")
    # 一次不带 transform（原图），一次带（缩略图）
    assert None in calls and server.IMG_THUMB_TRANSFORM in calls


@pytest.mark.asyncio
async def test_api_images_list_thumb_failure_falls_back_to_empty(bucket_mgr):
    await _make_photo(bucket_mgr, 1, "2026-07-01T00:00:00")

    async def fake_sign_urls(paths, expires_in=3600, transform=None):
        if transform:
            raise RuntimeError("Image Transformation 没开通")
        return {p: f"https://signed.example/{p}" for p in paths}

    with patch.object(server, "_require_auth", return_value=None), \
         patch.object(server, "bucket_mgr", bucket_mgr), \
         patch.object(server, "_img_is_configured", return_value=True), \
         patch("image_store.create_signed_urls", side_effect=fake_sign_urls):
        resp = await server.api_images_list(FakeRequest({"thumbs": "1"}))
    body = _json(resp)
    photo = body["photos"][0]
    # 缩略图签不出来不该 500——留空，前端退回 image_url
    assert photo["thumb_url"] == ""
    assert photo["image_url"] != ""


# --- image_store.create_signed_urls transform passthrough ---

class _FakeResp:
    def __init__(self, status_code, data):
        self.status_code = status_code
        self._data = data

    def json(self):
        return self._data


class _FakeAsyncClient:
    last_body = None

    def __init__(self, *a, **kw):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    async def post(self, url, headers=None, json=None):
        _FakeAsyncClient.last_body = json
        return _FakeResp(200, [
            {"path": p, "signedURL": f"/object/sign/{p}?token=x"} for p in json["paths"]
        ])


def test_create_signed_urls_includes_transform_when_given(monkeypatch):
    monkeypatch.setattr(image_store, "SUPABASE_URL", "https://x.supabase.co")
    monkeypatch.setattr(image_store, "SUPABASE_KEY", "key")
    monkeypatch.setattr(image_store.httpx, "AsyncClient", _FakeAsyncClient)

    import asyncio
    result = asyncio.run(image_store.create_signed_urls(
        ["a.jpg"], transform={"width": 320, "height": 320, "resize": "cover"}))

    assert _FakeAsyncClient.last_body["transform"] == {
        "width": 320, "height": 320, "resize": "cover"}
    assert result["a.jpg"].startswith("https://x.supabase.co/storage/v1")


def test_create_signed_urls_omits_transform_by_default(monkeypatch):
    monkeypatch.setattr(image_store, "SUPABASE_URL", "https://x.supabase.co")
    monkeypatch.setattr(image_store, "SUPABASE_KEY", "key")
    monkeypatch.setattr(image_store.httpx, "AsyncClient", _FakeAsyncClient)

    import asyncio
    asyncio.run(image_store.create_signed_urls(["a.jpg"]))
    assert "transform" not in _FakeAsyncClient.last_body
