import httpx
import pytest
import respx

from wswdy.clients.maptiler import (
    GeocodeError,
    geocode_address,
    render_static_map,
)


@respx.mock
async def test_geocode_returns_lat_lon_for_dc():
    respx.get(host="api.maptiler.com").mock(return_value=httpx.Response(200, json={
        "features": [{
            "place_name": "1500 14th St NW, Washington, DC, USA",
            "center": [-77.0319, 38.9097],
            "context": [{"id": "region", "text": "District of Columbia"}],
        }],
    }))
    out = await geocode_address("1500 14th St NW", api_key="K")
    assert out["lat"] == pytest.approx(38.9097)
    assert out["lon"] == pytest.approx(-77.0319)
    assert "Washington" in out["display"]


@respx.mock
async def test_geocode_no_results_raises():
    respx.get(host="api.maptiler.com").mock(return_value=httpx.Response(200, json={"features": []}))
    with pytest.raises(GeocodeError, match="no results"):
        await geocode_address("not a real address xyzzy", api_key="K")


@respx.mock
async def test_geocode_outside_dc_raises():
    respx.get(host="api.maptiler.com").mock(return_value=httpx.Response(200, json={
        "features": [{"place_name": "Baltimore, MD", "center": [-76.62, 39.29]}],
    }))
    with pytest.raises(GeocodeError, match="outside DC"):
        await geocode_address("Baltimore", api_key="K")


@respx.mock
async def test_render_static_map_writes_png(tmp_path):
    respx.get(host="api.maptiler.com").mock(
        return_value=httpx.Response(200, content=b"\x89PNG\r\n\x1a\n" + b"\x00" * 100,
                                    headers={"content-type": "image/png"})
    )
    out = tmp_path / "preview.png"
    await render_static_map(
        api_key="K", center_lat=38.9, center_lon=-77.0, radius_m=1000,
        markers=[(38.91, -77.03, 1), (38.90, -77.02, 4)],
        out_path=out, width=600, height=400,
    )
    assert out.exists()
    assert out.read_bytes().startswith(b"\x89PNG")
