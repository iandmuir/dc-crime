import pytest

from wswdy.tiers import classify, tier_label

CASES = [
    # offense, method, expected tier
    ("HOMICIDE",                     None,    1),
    ("HOMICIDE",                     "GUN",   1),
    ("SEX ABUSE",                    None,    1),
    ("ASSAULT W/DANGEROUS WEAPON",   "GUN",   1),
    ("ASSAULT W/DANGEROUS WEAPON",   "OTHERS",1),
    ("ROBBERY",                      "GUN",   1),  # armed → tier 1
    ("ROBBERY",                      "KNIFE", 1),
    ("ROBBERY",                      "OTHERS",2),  # unarmed → tier 2
    ("ROBBERY",                      None,    2),
    ("BURGLARY",                     None,    2),
    ("ARSON",                        None,    2),
    ("MOTOR VEHICLE THEFT",          None,    3),
    ("THEFT F/AUTO",                 None,    4),
    ("THEFT/OTHER",                  None,    4),
]


@pytest.mark.parametrize("offense,method,expected", CASES)
def test_classify(offense, method, expected):
    assert classify(offense, method) == expected


def test_classify_unknown_offense_defaults_to_4():
    assert classify("UNKNOWN OFFENSE", None) == 4


def test_classify_is_case_insensitive():
    assert classify("homicide", None) == 1
    assert classify("Robbery", "gun") == 1


def test_tier_labels():
    assert tier_label(1) == "violent"
    assert tier_label(2) == "serious property"
    assert tier_label(3) == "vehicle"
    assert tier_label(4) == "petty"
