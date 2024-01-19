import dcw.etl


def test_dcw_etl_imports():
    """Test that the dcw.etl module imports the correct names."""
    assert hasattr(dcw.etl, "ProcessingPipeline")
    assert hasattr(dcw.etl, "PipelineFactory")
    assert hasattr(dcw.etl, "run_pipeline")
    assert hasattr(dcw.etl, "Extractor")
    assert hasattr(dcw.etl, "Loader")
    assert hasattr(dcw.etl, "Transformation")
