import dcw


def test_dcw_exposes_ProcessingPipeline():
    assert hasattr(dcw, "ProcessingPipeline")
