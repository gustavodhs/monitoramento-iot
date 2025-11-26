from producer.producer import gen_sensor_event

def test_payload_keys():
    ev = gen_sensor_event()
    assert "ID_DISPOSITIVO" in ev
    assert "TEMPERATURA" in ev
    assert "UMIDADE" in ev
    assert "TIMESTAMP" in ev
    assert isinstance(ev["TEMPERATURA"], (float, int))
    assert isinstance(ev["LATITUDE"], float)
    assert isinstance(ev["LONGITUDE"], float)
