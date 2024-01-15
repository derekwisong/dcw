import dcw.system


def test_supports_colors_returns_bool():
    # Testing this function is a bit tricky because it depends on the platform and the terminal. Also, when running
    # tests using pytest, the terminal is not a tty, so when testing it would unlikely ever return true. So, instead
    # of testing the return value, just test that it returns a bool.
    assert type(dcw.system.supports_color()) == bool, "supports_color() should return a bool"
