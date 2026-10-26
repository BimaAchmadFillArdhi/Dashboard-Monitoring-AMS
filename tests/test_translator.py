import pytest
from app.translator.engine import TranslatorEngine


@pytest.fixture
def translator():
    return TranslatorEngine()


def test_translate_numerical_out_of_range(translator):
    raw = (
        "cannot write video frame to muxer(TC-DemenGlagah-VSS.m3u8). "
        "Pts: 598124250 dts:598124250  Error is Numerical result out of range"
    )
    res = translator.translate(raw, level="WARN")
    assert res.matched is True
    assert res.rule_id == "cannot_write_video_frame_numerical_range"
    assert res.severity == "masalah"
    assert "TC-DemenGlagah-VSS" in res.friendly_message
    assert "urutan waktu frame kacau" in res.friendly_message
    assert res.action_hint is not None


def test_translate_last_dts_bigger(translator):
    raw = (
        "Last dts:598133160 is bigger than incoming dts: 598096800 for stream index:0 "
        "and streamId:TC-DemenGlagah-VSS- If you see this log frequently and it's not related to playlist, "
        'you may TRY TO FIX it by setting "streamFetcherBufferTime"(to ie. 1000) in Application Settings'
    )
    res = translator.translate(raw, level="INFO")
    assert res.matched is True
    assert res.rule_id == "last_dts_bigger"
    assert "TC-DemenGlagah-VSS" in res.friendly_message
    assert "urutan waktu tidak teratur" in res.friendly_message
    assert res.severity == "perhatian"


def test_translate_audio_video_sync(translator):
    raw = "Audio/Video sync is more than 3500ms for stream:TC-DemenGlagah-VSS and trying to sync"
    res = translator.translate(raw, level="WARN")
    assert res.matched is True
    assert res.rule_id == "audio_video_sync_gap"
    assert "3500" in res.friendly_message
    assert "TC-DemenGlagah-VSS" in res.friendly_message


def test_translate_rtsp_connection_error(translator):
    raw = "Could not open input stream: Connection refused for streamId:kamera_selatan"
    res = translator.translate(raw, level="ERROR", stream_id="kamera_selatan")
    assert res.matched is True
    assert res.rule_id == "rtsp_connection_refused"
    assert res.severity == "masalah"
    assert "kamera_selatan" in res.friendly_message


def test_translate_stream_lifecycle(translator):
    start_raw = "Publishing started for streamId:APMD.stream"
    res_start = translator.translate(start_raw, level="INFO")
    assert res_start.rule_id == "stream_started"
    assert res_start.severity == "info"

    stop_raw = "Publishing stopped for streamId:APMD.stream"
    res_stop = translator.translate(stop_raw, level="INFO")
    assert res_stop.rule_id == "stream_finished"
    assert res_stop.severity == "perhatian"


def test_translate_unmatched_fallback(translator):
    raw_unknown = "Some completely unknown custom internal message 0xDEADBEEF"
    res = translator.translate(raw_unknown, level="INFO")
    assert res.matched is False
    assert res.rule_id is None
    assert res.friendly_title == "Pesan teknis belum diterjemahkan"
    assert res.friendly_message == raw_unknown
    assert res.severity == "tidak_dikenal"
