from wswdy.digest import build_digest_text, select_closest, summarize_by_tier

CRIMES = [
    {"offense": "ROBBERY", "method": "GUN", "block_address": "1400 block of P St NW",
     "lat": 38.9117, "lon": -77.0322, "report_dt": "2026-04-27T21:14:00Z"},
    {"offense": "MOTOR VEHICLE THEFT", "method": None,
     "block_address": "1200 block of 12th St NW",
     "lat": 38.9081, "lon": -77.0298, "report_dt": "2026-04-27T02:30:00Z"},
    {"offense": "THEFT F/AUTO", "method": None,
     "block_address": "1500 block of 14th St NW",
     "lat": 38.9100, "lon": -77.0319, "report_dt": "2026-04-27T03:48:00Z"},
]
HOME = (38.9097, -77.0319)


def test_summarize_by_tier_counts():
    s = summarize_by_tier(CRIMES)
    assert s == {1: 1, 2: 0, 3: 1, 4: 1}


def test_select_closest_within_half_radius():
    closest = select_closest(CRIMES, home_lat=HOME[0], home_lon=HOME[1],
                              radius_m=1000, max_items=3)
    # Half-radius is 500m. The theft from auto and the armed robbery are within.
    offenses = [c["offense"] for c in closest]
    assert "ROBBERY" in offenses and "THEFT F/AUTO" in offenses
    assert all(c["distance_m"] <= 500 for c in closest)
    # Sorted by distance ascending
    distances = [c["distance_m"] for c in closest]
    assert distances == sorted(distances)


def test_build_digest_text_includes_all_required_pieces():
    text = build_digest_text(
        display_name="Jane", radius_m=1000, crimes=CRIMES,
        home_lat=HOME[0], home_lon=HOME[1],
        map_url="https://x/map/abc?token=t",
        unsubscribe_url="https://x/u/abc?token=t",
        mpd_warning=False,
    )
    assert "Jane" in text
    assert "1000m" in text or "1,000m" in text
    assert "3 crimes reported" in text
    # tier counts
    assert "1 violent" in text
    assert "0 serious property" in text
    assert "1 vehicle" in text
    assert "1 petty" in text
    assert "https://x/map/abc?token=t" in text
    assert "https://x/u/abc?token=t" in text


def test_build_digest_zero_crimes_uses_quiet_phrasing():
    text = build_digest_text(
        display_name="Jane", radius_m=800, crimes=[],
        home_lat=HOME[0], home_lon=HOME[1],
        map_url="https://x/m", unsubscribe_url="https://x/u",
        mpd_warning=False,
    )
    assert "0 crimes reported" in text or "Quiet" in text or "no incidents" in text.lower()


def test_build_digest_appends_mpd_warning_when_flagged():
    text = build_digest_text(
        display_name="Jane", radius_m=1000, crimes=CRIMES,
        home_lat=HOME[0], home_lon=HOME[1],
        map_url="https://x/m", unsubscribe_url="https://x/u",
        mpd_warning=True,
    )
    assert "MPD data" in text or "delayed" in text.lower()


def test_select_closest_caps_at_max_items():
    many = [
        {"offense": "THEFT/OTHER", "method": None, "block_address": "x",
         "lat": 38.9098 + i * 0.0001, "lon": -77.0319, "report_dt": "2026-04-27T08:00:00Z"}
        for i in range(10)
    ]
    closest = select_closest(many, home_lat=HOME[0], home_lon=HOME[1],
                              radius_m=1000, max_items=3)
    assert len(closest) == 3
