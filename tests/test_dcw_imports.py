import dcw


def test_dcw_exposes_DataPipeline():
    assert hasattr(dcw, "DataPipeline")
