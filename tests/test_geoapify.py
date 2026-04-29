import httpx
import respx

from wswdy.clients.geoapify import render_static_map


@respx.mock
async def test_render_static_map_writes_png(tmp_path):
    respx.get(host="maps.geoapify.com").mock(
        return_value=httpx.Response(
            200,
            content=b"\x89PNG\r\n\x1a\n" + b"\x00" * 100,
            headers={"content-type": "image/png"},
        )
    )
    out = tmp_path / "preview.png"
    await render_static_map(
        api_key="K", center_lat=38.9, center_lon=-77.0, radius_m=1000,
        markers=[(38.91, -77.03, 1), (38.90, -77.02, 4)],
        out_path=out, width=600, height=400,
    )
    assert out.exists()
    assert out.read_bytes().startswith(b"\x89PNG")


@respx.mock
async def test_render_static_map_includes_home_pin_and_tier_markers(tmp_path):
    captured = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        return httpx.Response(200, content=b"\x89PNG\r\n\x1a\n",
                              headers={"content-type": "image/png"})

    respx.get(host="maps.geoapify.com").mock(side_effect=handler)
    out = tmp_path / "preview.png"
    await render_static_map(
        api_key="K", center_lat=38.9097, center_lon=-77.0319, radius_m=1000,
        markers=[(38.91, -77.03, 1)],
        out_path=out, width=600, height=400,
    )
    url = captured["url"]
    # Home pin uses the dark accent colour and is the first marker.
    assert "color%3A%230A0A0A" in url or "color:#0A0A0A" in url
    # Tier 1 marker (violent — red) is included.
    assert "color%3A%23DC2626" in url or "color:#DC2626" in url
    # Center coordinates land in the URL.
    assert "38.9097" in url and "-77.0319" in url


@respx.mock
async def test_render_static_map_caps_marker_count(tmp_path):
    """URL length is bounded — we cap to 50 markers regardless of input size."""
    captured = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        return httpx.Response(200, content=b"\x89PNG\r\n\x1a\n",
                              headers={"content-type": "image/png"})

    respx.get(host="maps.geoapify.com").mock(side_effect=handler)
    big = [(38.9 + i * 0.0001, -77.0, 1) for i in range(200)]
    await render_static_map(
        api_key="K", center_lat=38.9, center_lon=-77.0, radius_m=1000,
        markers=big, out_path=tmp_path / "p.png",
    )
    url = captured["url"]
    # Home pin (1) + 50 capped crime markers = 51 marker params.
    assert url.count("marker=") == 51
