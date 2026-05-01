"""Tests for offense / method humanization."""
import pytest

from wswdy.offenses import humanize_method, humanize_offense


@pytest.mark.parametrize("offense,method,expected", [
    ("HOMICIDE", None, "Homicide"),
    ("HOMICIDE", "GUN", "Homicide"),  # gun specified separately via humanize_method
    ("SEX ABUSE", None, "Sex abuse"),
    ("ASSAULT W/DANGEROUS WEAPON", "GUN", "Assault with Dangerous Weapon"),
    ("BURGLARY", None, "Burglary"),
    ("ARSON", None, "Arson"),
    ("MOTOR VEHICLE THEFT", None, "Motor Vehicle Theft"),
    ("THEFT/OTHER", None, "Theft (other)"),
    ("THEFT F/AUTO", None, "Theft from Auto"),
])
def test_humanize_offense_known_codes(offense, method, expected):
    assert humanize_offense(offense, method) == expected


def test_humanize_offense_robbery_armed():
    """Robbery with a weapon collapses to 'Armed robbery' for headline impact."""
    assert humanize_offense("ROBBERY", "GUN") == "Armed robbery"
    assert humanize_offense("ROBBERY", "KNIFE") == "Armed robbery"


def test_humanize_offense_robbery_unarmed():
    assert humanize_offense("ROBBERY", "OTHERS") == "Robbery"
    assert humanize_offense("ROBBERY", None) == "Robbery"
    assert humanize_offense("ROBBERY", "") == "Robbery"


def test_humanize_offense_unknown_falls_back_to_title_case():
    """A new offense MPD adds (e.g. 'CYBERCRIME') shouldn't render as
    SHOUTY UPPERCASE — title-case fallback is friendlier."""
    assert humanize_offense("CYBERCRIME") == "Cybercrime"
    assert humanize_offense("kidnapping") == "Kidnapping"


def test_humanize_offense_handles_none_and_empty():
    assert humanize_offense(None) == "Crime"
    assert humanize_offense("") == "Crime"


# ----- humanize_method ---------------------------------------------------

@pytest.mark.parametrize("method,expected", [
    ("GUN", "gun"),
    ("KNIFE", "knife"),
    ("gun", "gun"),
    ("Knife", "knife"),
    (" GUN ", "gun"),
])
def test_humanize_method_known(method, expected):
    assert humanize_method(method) == expected


@pytest.mark.parametrize("method", [
    None, "", "OTHERS", "Others", "UNKNOWN", "anything-else",
])
def test_humanize_method_returns_none_for_uninformative(method):
    """OTHERS, blanks, and unknown values all collapse to None so the
    popup just doesn't render a weapon row."""
    assert humanize_method(method) is None
