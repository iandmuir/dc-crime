"""Tests for email_reports — parsing MPD plain-text email bodies."""
from wswdy.email_reports import (
    detect_kind_from_subject,
    extract_district,
    parse_arrest_email,
    parse_crime_email,
)


# A trimmed-down version of the real MPD plain-text body the user shared,
# preserving the structural quirks: forwarded preamble, image alt-text,
# title sentence with a wrapped line, multiple records, an empty END DT,
# and the KEY: footer that must terminate parsing.
CRIME_BODY_2D = """\
---------- Forwarded message ---------
From: MPD News <mpd@subscriptions.dc.gov>
Date: Sun, May 3, 2026 at 7:51 AM
Subject: MPD: Preliminary Crime Report for 2D
To: <iandmuir@gmail.com>


[image: MPD Crime Blotter - 2D (Second District) Header]

This report contains information about recent crimes reported in the 2D
 District.
PSA 202
CCN 26058921
RPT DATE May 2, 2026 11:03:01 PM
OFFENSE Theft
METHOD Theft (Second Degree)
BLOCK 4200 BLOCK OF ALBEMARLE STREET NW
LOCATION Church Synagogue Temple Mosque
START DT May 2, 2026 2:11:00 AM
END DT May 2, 2026 5:01:00 AM
PSA 209
CCN 26058801
RPT DATE May 2, 2026 6:42:58 PM
OFFENSE Theft
METHOD Theft (Second Degree)
BLOCK 1200 BLOCK OF G STREET NW
LOCATION Department/Discount/Store
START DT May 2, 2026 5:16:00 PM
END DT
KEY:

PSA - The Police District ...
"""


# Sample arrest body — modeled on a real MPD email. Two structural
# quirks that make this format hostile to a naive parser:
#
#   1. NO "KEY:" footer — arrest emails just trail off into a
#      GovDelivery email signature.
#   2. The "Reminder:" / "Disclaimer:" / "Please Note:" boilerplate is
#      embedded IN THE MIDDLE of records, between the first batch of
#      arrests and the rest. The parser must keep going past it.
#
# A correct parser yields THREE records here, not just the first two.
ARREST_BODY_3D = """\
[image: MPD Arrest Blotter - 3D]

This report contains information about recent arrests reported in the 3D
 District.
Arrest Number# 022611844
Arrest Date & Time May 2, 2026 4:38:00 PM
Arrest Location 5028 BELT ROAD NW
WASHINGTON, DC 20016
UNITED STATES
Arrest Location PSA 202
Offender Last Name Slater
Offender First Name Alexander
Gender Male
Age 48
Offense Threat To Kidnap Or Injure A Person
Felony/Misdemeanor FELONY
Officer Doe 12345
Arrest Number# 022611855
Arrest Date & Time May 2, 2026 5:10:00 PM
Arrest Location 1100 NEW YORK AVE NW
WASHINGTON, DC 20002
UNITED STATES
Arrest Location PSA 209
Offender Last Name Marzi
Offender First Name Sadaf
Gender Female
Age 25
Offense Simple Assault
Felony/Misdemeanor MISDEMEANOR
Officer Smith 99999

Reminder:  Arrests do not constitute guilt in the criminal justice system.
Subjects will have an opportunity to defend these charges a court of law.
Disclaimer: This listing may contain duplicate records which may later be
cleaned by staff review.
Please Note: On 1/10/2019, MPD realigned police district boundaries.
Arrest Number# 022611888
Arrest Date & Time May 2, 2026 7:30:00 PM
Arrest Location 200 K STREET NW
WASHINGTON, DC 20001
UNITED STATES
Arrest Location PSA 207
Offender Last Name Doe
Offender First Name Jane
Gender Female
Age 30
Offense Bench Warrant
Felony/Misdemeanor FELONY
Officer Jones 13579

*Metropolitan Police Department*
Office of Communications
"""


# ---- subject helpers ----

def test_detect_kind_from_subject_crime():
    assert detect_kind_from_subject("MPD: Preliminary Crime Report for 2D") == "crime"
    # "Fwd:" prefix when forwarded for testing is fine
    assert detect_kind_from_subject("Fwd: MPD: Preliminary Crime Report for 2D") == "crime"


def test_detect_kind_from_subject_arrest():
    assert detect_kind_from_subject("MPD: Preliminary Arrest Report for 3D") == "arrest"


def test_detect_kind_from_unrelated_subject_is_none():
    assert detect_kind_from_subject("Hello world") is None
    assert detect_kind_from_subject("") is None


def test_extract_district_prefers_subject():
    """When subject and body claim different districts, subject wins —
    that's MPD's canonical labeling."""
    body = "This report contains information about recent crimes reported in the 1D District."
    assert extract_district(
        "MPD: Preliminary Crime Report for 2D", body,
    ) == "2D"


def test_extract_district_falls_back_to_body():
    body = "recent crimes reported in the 7D District"
    assert extract_district(None, body) == "7D"
    assert extract_district("Other subject", body) == "7D"


def test_extract_district_returns_none_when_neither_has_one():
    assert extract_district("Hello", "Nothing here") is None


# ---- crime parser ----

def test_parse_crime_email_returns_two_records():
    out = parse_crime_email(
        subject="MPD: Preliminary Crime Report for 2D",
        text_body=CRIME_BODY_2D,
    )
    assert [r.ccn for r in out] == ["26058921", "26058801"]


def test_parse_crime_email_district_from_subject():
    out = parse_crime_email(
        subject="MPD: Preliminary Crime Report for 2D",
        text_body=CRIME_BODY_2D,
    )
    assert all(r.district == "2D" for r in out)


def test_parse_crime_email_normalizes_dates_to_iso_utc():
    out = parse_crime_email(
        subject="MPD: Preliminary Crime Report for 2D",
        text_body=CRIME_BODY_2D,
    )
    # 11:03 PM EDT → 03:03 UTC next day
    assert out[0].rpt_date.startswith("2026-05-03T03:03:01")


def test_parse_crime_email_handles_blank_end_dt():
    """Second record has 'END DT' with no value — should land as None."""
    out = parse_crime_email(
        subject="MPD: Preliminary Crime Report for 2D",
        text_body=CRIME_BODY_2D,
    )
    assert out[1].end_dt is None


def test_parse_crime_email_stops_at_key_footer():
    """The legend is full of words like 'PSA' and 'CCN' — make sure the
    parser terminates at 'KEY:' so it doesn't pick those up."""
    out = parse_crime_email(
        subject="MPD: Preliminary Crime Report for 2D",
        text_body=CRIME_BODY_2D,
    )
    assert len(out) == 2  # exactly the two real records, no legend rows


def test_parse_crime_email_extracts_pdf_only_location_field():
    """The new LOCATION enum (not in the API feed) is the whole reason
    we're parsing these emails — make sure it lands."""
    out = parse_crime_email(
        subject="MPD: Preliminary Crime Report for 2D",
        text_body=CRIME_BODY_2D,
    )
    assert out[0].location == "Church Synagogue Temple Mosque"
    assert out[1].location == "Department/Discount/Store"


# ---- arrest parser ----

def test_parse_arrest_email_returns_all_records_across_boilerplate():
    """Boilerplate appears mid-email — must NOT terminate parsing."""
    out = parse_arrest_email(
        subject="MPD: Preliminary Arrest Report for 3D",
        text_body=ARREST_BODY_3D,
    )
    assert [r.arrest_number for r in out] == [
        "022611844", "022611855", "022611888",
    ]


def test_parse_arrest_email_tags_district_from_subject():
    out = parse_arrest_email(
        subject="MPD: Preliminary Arrest Report for 3D",
        text_body=ARREST_BODY_3D,
    )
    assert all(r.district == "3D" for r in out)


def test_parse_arrest_email_does_not_pollute_officer_with_boilerplate():
    """Regression: the 'Reminder:' / 'Disclaimer:' / 'Please Note:'
    paragraphs sit BETWEEN arrest records (not after them). A bad
    continuation rule could append all that prose to the previous
    record's officer field — so explicitly guard against it."""
    out = parse_arrest_email(
        subject="MPD: Preliminary Arrest Report for 3D",
        text_body=ARREST_BODY_3D,
    )
    for r in out:
        assert r.officer is not None
        assert "Reminder" not in r.officer
        assert "Disclaimer" not in r.officer
        assert "Please Note" not in r.officer
    assert out[1].officer == "Smith 99999"


def test_parse_arrest_email_extracts_full_arrestee_record():
    out = parse_arrest_email(
        subject="MPD: Preliminary Arrest Report for 3D",
        text_body=ARREST_BODY_3D,
    )
    a = out[0]
    assert a.offender_first_name == "Alexander"
    assert a.offender_last_name == "Slater"
    assert a.age == 48
    assert a.felony_misdemeanor == "FELONY"
    assert a.officer == "Doe 12345"


def test_parse_arrest_email_joins_multi_line_address_in_body():
    """The default fixture above puts the address across 3 lines —
    verify the canonical example also assembles correctly."""
    out = parse_arrest_email(
        subject="MPD: Preliminary Arrest Report for 3D",
        text_body=ARREST_BODY_3D,
    )
    assert "5028 BELT ROAD NW" in out[0].arrest_location
    assert "WASHINGTON, DC 20016" in out[0].arrest_location
    assert "UNITED STATES" in out[0].arrest_location


# (Multi-line address parsing is now covered by
#  test_parse_arrest_email_joins_multi_line_address_in_body — the
#  default fixture itself has the address split across 3 lines.)


def test_parse_arrest_email_joins_wrapped_offense():
    """Regression: long offenses (e.g. 'Carrying a Pistol Without a
    License (Outside Home or Place of Business)') wrap mid-parenthetical
    in MPD's emails. The closing parenthesis must survive."""
    body = ARREST_BODY_3D.replace(
        "Offense Threat To Kidnap Or Injure A Person",
        "Offense Carrying a Pistol Without a License (Outside Home or Place of\nBusiness)",
    )
    out = parse_arrest_email(
        subject="MPD: Preliminary Arrest Report for 3D",
        text_body=body,
    )
    assert out[0].offense == (
        "Carrying a Pistol Without a License (Outside Home or Place of Business)"
    )


def test_parse_arrest_email_handles_abbreviated_month():
    """Regression: MPD's templates use 3-letter month abbreviations
    ("Apr 30, ..."). Earlier we only had %B (full month) format strings,
    which silently dropped dates for every month except May (since 'May'
    is identical in both forms). Both forms must round-trip."""
    body_apr = ARREST_BODY_3D.replace(
        "May 2, 2026 4:38:00 PM",
        "Apr 30, 2026 7:00:00 AM",
    )
    out = parse_arrest_email(
        subject="MPD: Preliminary Arrest Report for 3D",
        text_body=body_apr,
    )
    assert out[0].arrest_dt is not None
    # 7 AM EDT == 11:00 UTC
    assert out[0].arrest_dt.startswith("2026-04-30T11:00:00")


def test_parse_arrest_email_captures_offense():
    """Regression: 'Offense' (arrest label) collided case-insensitively
    with 'OFFENSE' (crime label), causing the arrest offense to be
    silently dropped. Both records should round-trip with their full
    offense string intact."""
    out = parse_arrest_email(
        subject="MPD: Preliminary Arrest Report for 3D",
        text_body=ARREST_BODY_3D,
    )
    assert out[0].offense == "Threat To Kidnap Or Injure A Person"
    assert out[1].offense == "Simple Assault"


def test_parse_arrest_email_handles_unparseable_age(monkeypatch):
    body = ARREST_BODY_3D.replace("Age 48", "Age unknown")
    out = parse_arrest_email(
        subject="MPD: Preliminary Arrest Report for 3D",
        text_body=body,
    )
    assert out[0].age is None


def test_parse_arrest_email_handles_no_key_footer():
    """Arrest emails have no 'KEY:' legend at all — they trail off
    into the GovDelivery email signature. Parser should still
    terminate cleanly at end-of-text without hanging on signature
    lines."""
    out = parse_arrest_email(
        subject="MPD: Preliminary Arrest Report for 3D",
        text_body=ARREST_BODY_3D,
    )
    assert len(out) == 3
    assert out[-1].arrest_number == "022611888"
    # Trailing signature lines must NOT have leaked into the last
    # record's last field.
    assert out[-1].officer == "Jones 13579"
