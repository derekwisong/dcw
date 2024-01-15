from dcw.etl.transform import SquareTransformation


def test_square():
    """Test that the a simple transformation that squares output works as expected."""
    input = [1, 2, 3]
    output = [1, 4, 9]
    transformation = SquareTransformation()
    transformed = [transformation.transform(i) for i in input]
    assert transformed == output, f"Transformed data does not match: {transformed} != {output}"


def test_transformation_is_callable():
    """Test that calling a transformation object with an input value returns an output."""
    input = 10
    expected = 100
    transformation = SquareTransformation()
    transformed = transformation(input)  # invoke the __call__(item) method
    assert transformed == expected, f"Expected to obtain squared value ({expected}) by calling the object"
