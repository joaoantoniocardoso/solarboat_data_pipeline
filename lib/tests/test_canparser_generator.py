from lib.canparser_generator import CanTopicParser
from math import isclose


def test_apply_units():
    # Test case 1: No units, no scaling
    units, value = CanTopicParser.apply_units("", 42)
    assert units == ""
    assert isclose(value, 42)

    # Test case 2: Percentage units, scaling
    units, value = CanTopicParser.apply_units("%", 128)
    assert units == "%"
    assert isclose(value, 0.5, rel_tol=0.01)

    # Test case 3: Custom units, scaling
    units, value = CanTopicParser.apply_units("A/100", 1500)
    assert units == "A"
    assert isclose(value, 15.0, rel_tol=0.01)
