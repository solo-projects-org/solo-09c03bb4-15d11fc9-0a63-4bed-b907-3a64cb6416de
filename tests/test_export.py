"""Window-export tests: replayability, baseline carry-over, ordering, atomicity."""

from __future__ import annotations

import csv
import io
import json
import re
import sys
import tempfile
import threading
import zipfile
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app import db as dbmod
from app import export as exportmod
from app import main as mainmod
from app.store import Store

BASE = 1767225600
HEADER = "station,mmsi,utc,lat,lon\n"
SAMPLES = Path(__file__).resolve().parent.parent / "samples"


@pytest.fixture()
def client(tmp_path, monkeypatch):
    monkeypatch.setattr(mainmod, "DATA_DIR", tmp_path / "data")
    monkeypatch.setattr(mainmod, "DB_PATH", tmp_path / "data" / "ais.sqlite3")
    mainmod.DATA_DIR.mkdir(parents=True, exist_ok=True)
    with TestClient(mainmod.app) as c:
        yield c


def upload(client, name, body):
    payload = body.encode() if isinstance(body, str) else body
    return client.post(
        "/upload",
        files={"file": (name, io.BytesIO(payload), "text/csv")},
        follow_redirects=False,
    )


def publish_samples(client):
    assert upload(client, "batch1_base.csv", (SAMPLES / "batch1_base.csv").read_bytes()).status_code == 303
    assert upload(
        client,
        "batch2_late_arrivals.csv",
        (SAMPLES / "batch2_late_arrivals.csv").read_bytes(),
    ).status_code == 303


def fetch_zip(client, mmsi, start, end):
    resp = client.post("/exports/window", data={"mmsi": mmsi, "start": str(start), "end": str(end)})
    return resp


def read_zip(resp):
    assert resp.status_code == 200, resp.text
    assert resp.headers["content-type"] == "application/zip"
    archive = zipfile.ZipFile(io.BytesIO(resp.content))
    return archive


def manifest_rows(archive):
    text = archive.read("manifest.csv").decode("utf-8")
    return list(csv.DictReader(io.StringIO(text)))


def replay_in_empty_db(tmp_path, track_bytes):
    """Import the exported CSV through the same store path as an upload."""

    root = Path(tempfile.mkdtemp(dir=tmp_path, prefix="replay-"))
    dbmod.init_db(root / "ais.sqlite3")
    conn = dbmod.connect(root / "ais.sqlite3")
    try:
        store = Store(root, conn)
        batch = store.publish("track.csv", track_bytes)
        mmsi_rows = conn.execute(
            "SELECT DISTINCT mmsi FROM raw_rows WHERE batch_id = ?", (batch.batch_id,)
        ).fetchall()
        assert len(mmsi_rows) == 1
        mmsi = mmsi_rows[0]["mmsi"]
        version = conn.execute(
            "SELECT version FROM vessels WHERE mmsi = ?", (mmsi,)
        ).fetchone()["version"]
        rows = conn.execute(
            """
            SELECT r.line_no, v.status, v.reason, v.detail
            FROM verdicts v JOIN raw_rows r ON r.id = v.raw_row_id
            WHERE v.mmsi = ? AND v.version = ?
            """,
            (mmsi, version),
        ).fetchall()
        return {
            r["line_no"]: (r["status"], r["reason"], r["detail"]) for r in rows
        }
    finally:
        conn.close()


def assert_replays(tmp_path, resp, mmsi):
    archive = read_zip(resp)
    track = archive.read("track.csv")
    replayed = replay_in_empty_db(tmp_path, track)
    window = [r for r in manifest_rows(archive) if r["role"] == "window"]
    assert window, "window must contain at least one record"
    for row in window:
        line = int(row["csv_line"])
        assert line in replayed, f"csv line {line} missing after replay"
        status, reason, detail = replayed[line]
        assert status == row["status"], (row, status)
        assert (reason or "") == row["reason"], (row, reason)
        assert (detail or "") == row["detail"], (row, detail)
    return archive, track, window, replayed


# --------------------------------------------------------------------------
# Replay fidelity across every rule
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    "mmsi,start,end",
    [
        ("413000001", BASE, BASE + 300),   # duplicates from re-sent telegrams
        ("413000002", BASE, BASE + 300),   # same-second conflict
        ("413000003", BASE, BASE + 300),   # speed gate
        ("413000005", BASE, BASE + 300),   # late arrival, trusted then isolated
        ("413000006", BASE, BASE + 4000),  # late row flips an earlier verdict
    ],
)
def test_every_window_record_replays_with_same_verdict(client, tmp_path, mmsi, start, end):
    publish_samples(client)
    resp = fetch_zip(client, mmsi, start, end)
    assert_replays(tmp_path, resp, mmsi)


def test_manifest_matches_live_page_and_carries_sources(client, tmp_path):
    publish_samples(client)
    resp = fetch_zip(client, "413000003", BASE, BASE + 300)
    archive, track, window, replayed = assert_replays(tmp_path, resp, "413000003")

    assert {r["status"] for r in window} == {"trusted", "isolated"}
    isolated = [r for r in window if r["status"] == "isolated"]
    assert {r["reason"] for r in isolated} == {"speed_exceeded"}
    for row in window:
        assert row["batch_id"] and row["batch_filename"]
        assert int(row["original_line"]) > 0

    # The verdict rows on the vessel page agree with the manifest.
    page = client.get("/vessels/413000003")
    assert page.status_code == 200
    assert "speed_exceeded" in page.text

    # CSV lines in the manifest actually point at the matching rows.
    track_lines = track.decode("utf-8").splitlines()
    window_by_utc = [(int(r["utc"]), int(r["csv_line"])) for r in window]
    assert window_by_utc == sorted(window_by_utc, key=lambda item: item[0])
    for _, line in window_by_utc:
        parsed = next(csv.DictReader(io.StringIO(track_lines[0] + "\n" + track_lines[line - 1] + "\n")))
        assert parsed["mmsi"] == "413000003"
        assert int(parsed["utc"]) in {u for u, _ in window_by_utc}


def test_baseline_is_required_to_reproduce_speed_verdict(client, tmp_path):
    """The window starts after a trusted point; dropping it would flip a verdict."""

    publish_samples(client)
    resp = fetch_zip(client, "413000003", BASE + 60, BASE + 120)
    archive, track, window, replayed = assert_replays(tmp_path, resp, "413000003")

    baseline = [r for r in manifest_rows(archive) if r["role"] == "baseline"]
    assert len(baseline) == 1
    assert int(baseline[0]["utc"]) == BASE
    assert len(window) == 1 and window[0]["status"] == "isolated"

    # Without the baseline this single row would look trustworthy.
    lines = track.decode("utf-8").splitlines()
    without = ("\n".join([lines[0]] + lines[2:]) + "\n").encode()
    now = Path(tempfile.mkdtemp(dir=tmp_path, prefix="baseline-check-"))
    replayed = replay_in_empty_db(now, without)
    assert replayed[2][0] == "trusted"


def test_no_baseline_when_nothing_trusted_before_window(client, tmp_path):
    publish_samples(client)
    # 413000002 has no trusted pre-window point at all (its only second conflicts).
    resp = fetch_zip(client, "413000002", BASE, BASE + 60)
    archive, track, window, _ = assert_replays(tmp_path, resp, "413000002")
    assert [r for r in manifest_rows(archive) if r["role"] == "baseline"] == []
    assert len(window) == 4  # batch 1 sends three, batch 2 re-sends one

    # Starting one second later leaves an empty window.
    empty = fetch_zip(client, "413000002", BASE + 60, BASE + 120)
    assert empty.status_code == 404
    assert "zip" not in empty.headers.get("content-type", "")


def test_whole_second_is_never_split_by_the_window(client, tmp_path):
    """All rows of a conflicting second are either in or out, together."""

    publish_samples(client)
    resp = fetch_zip(client, "413000002", BASE, BASE + 1)  # one-second window
    archive, track, window, _ = assert_replays(tmp_path, resp, "413000002")
    assert len(window) == 4  # all four records of the contested second
    assert {r["reason"] for r in window} == {"conflict"}


def test_duplicate_arrival_order_survives_replay(client, tmp_path):
    """The first-arriving row of a re-sent telegram stays the trusted winner."""

    publish_samples(client)
    resp = fetch_zip(client, "413000001", BASE + 60, BASE + 61)
    archive, track, window, _ = assert_replays(tmp_path, resp, "413000001")

    by_station = {r["station"]: r for r in window if r["batch_filename"] == "batch1_base.csv"}
    assert set(by_station) == {"SZ01", "HK02"}
    assert by_station["SZ01"]["status"] == "trusted"
    assert by_station["HK02"]["status"] == "isolated"
    assert by_station["HK02"]["reason"] == "duplicate"
    # Same-second rows are listed in arrival order, matching the CSV.
    same_second = [r for r in window if r["batch_filename"] == "batch1_base.csv"]
    assert [int(r["csv_line"]) for r in same_second] == sorted(
        int(r["csv_line"]) for r in same_second
    )
    # The re-send in the later batch stays isolated as well.
    late = [r for r in window if r["batch_filename"] == "batch2_late_arrivals.csv"]
    assert [r["status"] for r in late] == ["isolated"]

    # Replay in the exported file order picks the same winner.
    lines = track.decode("utf-8").splitlines()
    csved = list(csv.reader(io.StringIO("\n".join(lines))))
    assert csved[2][0] == "SZ01"  # first window row is the winner


def test_window_only_contains_parsed_records_of_the_vessel(client, tmp_path):
    publish_samples(client)
    resp = fetch_zip(client, "413000004", BASE, BASE + 300)
    archive = read_zip(resp)
    rows = list(csv.DictReader(io.StringIO(archive.read("track.csv").decode())))
    assert {r["mmsi"] for r in rows} == {"413000004"}
    # The rejected parse lines never enter the package.
    assert len(rows) == 1

    # All-good rows replays too.
    assert_replays(tmp_path, resp, "413000004")


def test_package_metadata_and_checksums(client):
    publish_samples(client)
    resp = fetch_zip(client, "413000005", BASE, BASE + 300)
    archive = read_zip(resp)
    assert set(archive.namelist()) == {
        "track.csv",
        "manifest.csv",
        "package.json",
        "README.txt",
    }
    meta = json.loads(archive.read("package.json"))
    assert meta["format"] == "ais-window-export-v1"
    assert meta["vessel_mmsi"] == "413000005"
    assert meta["window"] == {
        "start_utc": BASE,
        "end_utc": BASE + 300,
        "start_inclusive": True,
        "end_exclusive": True,
        "start_iso": "2026-01-01T00:00:00+00:00",
        "end_iso": "2026-01-01T00:05:00+00:00",
    }
    assert meta["track_version"] == int(resp.headers["x-track-version"])
    for name in ("track.csv", "manifest.csv"):
        payload = archive.read(name)
        assert meta["files"][name]["bytes"] == len(payload)
        assert meta["files"][name]["sha256"] == exportmod._sha256(payload)
    assert "manifest.csv" in archive.read("README.txt").decode()


# --------------------------------------------------------------------------
# Input handling and failure atomicity
# --------------------------------------------------------------------------


def test_iso_and_epoch_bounds_are_equivalent(client):
    publish_samples(client)
    epoch = fetch_zip(client, "413000001", BASE, BASE + 60)
    iso = client.post(
        "/exports/window",
        data={"mmsi": "413000001", "start": "2026-01-01T00:00:00Z", "end": "2026-01-01T00:01:00Z"},
    )
    assert iso.status_code == 200
    assert manifest_rows(zipfile.ZipFile(io.BytesIO(epoch.content))) == manifest_rows(
        zipfile.ZipFile(io.BytesIO(iso.content))
    )
    # GET works too, and offsets are honoured.
    shifted = client.get(
        "/exports/window",
        params={"mmsi": "413000001", "start": "2026-01-01T08:00:00+08:00", "end": "2026-01-01T08:01:00+08:00"},
    )
    assert shifted.status_code == 200


def test_invalid_bounds_are_refused_without_a_package(client):
    publish_samples(client)
    bad_start = fetch_zip(client, "413000001", "not-a-time", BASE)
    assert bad_start.status_code == 400
    assert "压缩包" not in bad_start.headers.get("content-type", "")

    reversed_window = fetch_zip(client, "413000001", BASE + 60, BASE)
    assert reversed_window.status_code == 400

    unknown = fetch_zip(client, "999999999", BASE, BASE + 60)
    assert unknown.status_code == 404


def test_csv_dialect_survives_station_names(client, tmp_path):
    """Names with delimiters/quotes must not change the CSV the reviewer sees."""

    stations = ["SZ,01", "A;B", "C\tD", "E|F", 'G "H"']
    body = HEADER + "".join(
        f'"{st.replace(chr(34), chr(34) * 2)}",413000077,{BASE + i * 60},22.{i:06d},114.{i:06d}\n'
        for i, st in enumerate(stations)
    )
    assert upload(client, "names.csv", body).status_code == 303
    resp = fetch_zip(client, "413000077", BASE, BASE + 300)
    archive, track, window, _ = assert_replays(tmp_path, resp, "413000077")
    assert {r["station"] for r in window} == set(stations)
    assert len(window) == len(stations)


def test_package_replays_through_the_normal_upload_endpoint(client, tmp_path, monkeypatch):
    """The reviewer's actual path: download ZIP, upload track.csv to an empty app."""

    publish_samples(client)
    resp = fetch_zip(client, "413000006", BASE + 1, BASE + 3600 + 1)
    archive = read_zip(resp)
    track = archive.read("track.csv")
    window = [r for r in manifest_rows(archive) if r["role"] == "window"]

    fresh = tmp_path / "fresh-data"
    monkeypatch.setattr(mainmod, "DATA_DIR", fresh)
    monkeypatch.setattr(mainmod, "DB_PATH", fresh / "ais.sqlite3")
    fresh.mkdir(parents=True, exist_ok=True)
    with TestClient(mainmod.app) as c2:
        replay = upload(c2, "track.csv", track)
        assert replay.status_code == 303, replay.text
        page = c2.get("/vessels/413000006")
        assert page.status_code == 200
        csv_rows = {
            int(r["original_line"]): r
            for r in csv.DictReader(io.StringIO(c2.get("/vessels/413000006/isolated.csv").text))
        }
    assert len(csv_rows) == sum(1 for r in window if r["status"] == "isolated")
    for line, row in csv_rows.items():
        expected = next(r for r in window if int(r["csv_line"]) == line)
        assert row["reason"] == expected["reason"]
        assert row["detail"] == expected["detail"]


def test_export_page_preselects_vessel(client):
    publish_samples(client)
    page = client.get("/export", params={"mmsi": "413000006"})
    assert page.status_code == 200
    assert re.search(r'value="413000006"[^>]*selected', page.text)


def test_export_page_lists_vessels(client):
    publish_samples(client)
    page = client.get("/export")
    assert page.status_code == 200
    assert "413000001" in page.text
    assert "/exports/window" in page.text


def test_window_too_large_for_reupload_is_refused(client, monkeypatch):
    """A track.csv the reviewer could not upload is not offered as replayable."""

    publish_samples(client)
    monkeypatch.setattr(mainmod, "MAX_UPLOAD_BYTES", 64)
    resp = fetch_zip(client, "413000006", BASE, BASE + 4000)
    assert resp.status_code == 413
    assert "缩小" in resp.text
    assert "zip" not in resp.headers.get("content-type", "")


def test_failed_generation_returns_no_zip(client, monkeypatch):
    publish_samples(client)

    def boom(*args, **kwargs):
        raise exportmod.ExportError(500, "boom")

    monkeypatch.setattr(mainmod, "build_window_package", boom)
    resp = fetch_zip(client, "413000001", BASE, BASE + 60)
    assert resp.status_code == 500
    assert resp.headers["content-type"].startswith("text/html")
    assert "boom" in resp.text


def test_export_does_not_disturb_published_state(client):
    publish_samples(client)
    before = client.get("/vessels/413000001/track.csv").content
    fetch_zip(client, "413000001", BASE, BASE + 300)
    after = client.get("/vessels/413000001/track.csv").content
    assert before == after


# --------------------------------------------------------------------------
# Concurrency: one published version per package
# --------------------------------------------------------------------------


def test_concurrent_upload_never_mixes_versions(client, tmp_path):
    """Exports racing an upload still replay to themselves."""

    publish_samples(client)
    stop = threading.Event()
    errors: list[str] = []

    def uploader():
        i = 0
        while not stop.is_set():
            i += 1
            body = HEADER + (
                f"HK02,413000009,{BASE + 300 + i * 60},{21.5 + i * 0.001:.6f},113.5\n"
            )
            resp = upload(client, f"race{i}.csv", body)
            if resp.status_code != 303:
                errors.append(f"upload {resp.status_code}")

    thread = threading.Thread(target=uploader)
    thread.start()
    try:
        for _ in range(12):
            resp = fetch_zip(client, "413000009", BASE, BASE + 900)
            if resp.status_code == 404:
                continue
            archive, track, window, _ = assert_replays(tmp_path, resp, "413000009")
            assert {r["batch_id"] for r in window}  # all from committed batches
            meta = json.loads(archive.read("package.json"))
            assert meta["track_version"] == int(resp.headers["x-track-version"])
    finally:
        stop.set()
        thread.join()

    assert errors == []


def test_snapshot_pins_a_single_vessel_version_directly(client, tmp_path):
    """Between two publishes, every artefact comes from one version."""

    publish_samples(client)
    conn = dbmod.connect(mainmod.DB_PATH)
    try:
        with dbmod.read_tx(conn):
            version = conn.execute(
                "SELECT version FROM vessels WHERE mmsi='413000006'"
            ).fetchone()["version"]
            package = exportmod.build_window_package(conn, "413000006", BASE, BASE + 4000)
            # A concurrent publish cannot affect this snapshot.
            version_after = conn.execute(
                "SELECT version FROM vessels WHERE mmsi='413000006'"
            ).fetchone()["version"]
        assert version == version_after
    finally:
        conn.close()
    assert package.version == version
    archive = zipfile.ZipFile(io.BytesIO(package.data))
    meta = json.loads(archive.read("package.json"))
    assert meta["track_version"] == version


def test_self_check_catches_unreplayable_windows(client, tmp_path, monkeypatch):
    """If trimming could not be replayed, generation fails instead of shipping."""

    publish_samples(client)

    def broken(track_bytes, payload, line_numbers, mmsi):
        raise exportmod.ExportError(500, "导出校验失败")

    monkeypatch.setattr(exportmod, "self_check_replay", broken)
    resp = fetch_zip(client, "413000003", BASE, BASE + 120)
    assert resp.status_code == 500
    assert "zip" not in resp.headers["content-type"]


# --------------------------------------------------------------------------
# Rule-level: the pure evaluator tolerates a lone baseline row
# --------------------------------------------------------------------------


def test_evaluator_baseline_then_window_matches_full_run():
    from app import ais

    def cand(i, utc, lat, lon, station="SZ01"):
        return ais.Candidate(row_id=i, station=station, utc=utc, lat=lat, lon=lon)

    full = [
        cand(1, 0, 22.0, 114.0),
        cand(2, 60, 22.9, 114.9),   # isolated: absurd speed
        cand(3, 120, 22.001, 114.0),
        cand(4, 180, 22.9, 114.9),  # compared with t=120, still absurd
    ]
    everything = ais.evaluate_track(full)
    trimmed = ais.evaluate_track([cand(1, 0, 22.0, 114.0), cand(3, 120, 22.001, 114.0)])
    # Only the trusted pre-window point is needed to reproduce the window.
    assert everything.candidate_status[3] == trimmed.candidate_status[3]
    assert trimmed.candidate_status[3][0] == "trusted"
