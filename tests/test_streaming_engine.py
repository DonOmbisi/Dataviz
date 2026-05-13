from streaming_engine import DataStreamBuffer


def test_buffer_add_and_dataframe():
    buf = DataStreamBuffer(max_size=100)
    assert buf.add_record({"x": 1, "y": "a"}) is True
    assert buf.add_batch([{"x": 2}, {"x": 3}])["records_added"] == 2
    df = buf.get_dataframe()
    assert len(df) == 3
    assert list(buf.get_recent(2)[-1].keys()) == ["x"]
