# tests/test_payload_schema.py
from producer.producer import gen_sensor_event

def test_payload_keys():
    ev = gen_sensor_event()
    assert "device_id" in ev
    assert "temperature" in ev
    assert "humidity" in ev
    assert "timestamp" in ev
    assert isinstance(ev["temperature"], (float, int))
    assert isinstance(ev["latitude"], float)
    assert isinstance(ev["longitude"], float)
