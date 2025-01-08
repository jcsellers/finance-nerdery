import pytest


def test_addition():
    assert 1 + 1 == 2


def test_subtraction():
    assert 5 - 3 == 2


def test_multiplication():
    assert 2 * 3 == 6


def test_division():
    assert 10 / 2 == 5


def test_string_concatenation():
    result = "Hello" + " " + "World"
    assert result == "Hello World"


# Additional placeholder tests
def test_uppercase():
    string = "hello"
    assert string.upper() == "HELLO"


def test_lowercase():
    string = "HELLO"
    assert string.lower() == "hello"


def test_isdigit():
    string = "12345"
    assert string.isdigit() is True


def test_split():
    string = "a,b,c"
    assert string.split(",") == ["a", "b", "c"]
