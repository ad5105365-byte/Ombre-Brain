# ============================================================
# Test: 内部钩子端点的"仅限本机"守卫 — _is_local_request / _require_local
# 背景（2026-07-18 安全加固纵深）：nginx 已 deny 公网访问内部钩子，
# 这层是 app 侧双保险。nginx 反代后公网流量源 IP 也是 127.0.0.1，
# 靠"有没有转发头"区分：带 X-Forwarded-For/X-Real-IP = 经反代 = 拒。
# ============================================================

import pytest

import server


class FakeClient:
    def __init__(self, host):
        self.host = host


class FakeRequest:
    def __init__(self, host="127.0.0.1", headers=None):
        self.client = FakeClient(host) if host is not None else None
        self.headers = {k.lower(): v for k, v in (headers or {}).items()}


# --- 纯判定 _is_local_request ---

def test_local_direct_loopback_allowed():
    assert server._is_local_request("127.0.0.1", {}) is True
    assert server._is_local_request("::1", {}) is True


def test_public_direct_ip_denied():
    assert server._is_local_request("113.84.1.2", {}) is False
    assert server._is_local_request(None, {}) is False


def test_proxied_traffic_denied_even_from_loopback():
    """nginx 反代来的公网请求：源是 127.0.0.1 但带转发头 → 必须拒。"""
    assert server._is_local_request("127.0.0.1", {"x-forwarded-for": "1.2.3.4"}) is False
    assert server._is_local_request("127.0.0.1", {"x-real-ip": "1.2.3.4"}) is False


def test_forged_forward_header_from_outside_still_denied():
    """公网直连伪造转发头（真到得了的话）也照样拒。"""
    assert server._is_local_request("5.6.7.8", {"x-forwarded-for": "127.0.0.1"}) is False


# --- _require_local 包装 ---

def test_require_local_allows_local():
    assert server._require_local(FakeRequest()) is None


def test_require_local_403_for_proxied():
    resp = server._require_local(FakeRequest(headers={"X-Forwarded-For": "1.2.3.4"}))
    assert resp is not None and resp.status_code == 403


def test_require_local_403_no_client():
    resp = server._require_local(FakeRequest(host=None))
    assert resp is not None and resp.status_code == 403


# --- 端点接线：guard 在最前面，403 时不执行任何业务逻辑 ---

@pytest.mark.asyncio
async def test_breath_hook_guarded():
    resp = await server.breath_hook(FakeRequest(headers={"X-Forwarded-For": "1.2.3.4"}))
    assert resp.status_code == 403


@pytest.mark.asyncio
async def test_recall_hook_guarded():
    resp = await server.recall_hook(FakeRequest(headers={"X-Real-IP": "1.2.3.4"}))
    assert resp.status_code == 403


@pytest.mark.asyncio
async def test_dream_hook_guarded():
    resp = await server.dream_hook(FakeRequest(headers={"X-Forwarded-For": "1.2.3.4"}))
    assert resp.status_code == 403


@pytest.mark.asyncio
async def test_ferry_hook_guarded():
    resp = await server.ferry_hook(FakeRequest(headers={"X-Forwarded-For": "1.2.3.4"}))
    assert resp.status_code == 403


@pytest.mark.asyncio
async def test_hook_log_guarded():
    resp = await server.hook_log(FakeRequest(headers={"X-Forwarded-For": "1.2.3.4"}))
    assert resp.status_code == 403
