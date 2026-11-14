"""Replayable window export for an external review team.

The reviewer only needs to see *why* a slice of a track was judged the way it
was.  The package therefore contains:

* ``track.csv`` - a single-vessel CSV accepted by the normal ``POST /upload``
  endpoint.  It holds every parsed row of the window ``[start, end)`` **plus
  the last trusted point before the window**, when one exists.  That one point
  is the only pre-window state the evaluator carries forward (the speed gate
  compares against the previous *trusted* point), so replaying the file in an
  empty database reproduces every window verdict exactly.
* ``manifest.csv`` - the window verdicts at export time, each tied to its
  original batch, original line number and the line it occupies in
  ``track.csv``.
* ``package.json`` / ``README.txt`` - metadata and replay instructions.

Selection rules that keep replay faithful
-----------------------------------------
* Whole UTC seconds are selected; a same-second conflict or re-send is never
  split by the window boundary.
* Rows are emitted in original arrival order (``raw_rows.id``).  Duplicate
  winners are decided by earliest arrival, so preserving that relative order
  preserves the same winner after the ids are reassigned on import.
* Rejected parse lines are never exported: they never influence a verdict.

Nothing is streamed straight out of the database: the ZIP is assembled in
memory and self-checked against a pure replay of the exported rows.  If that
check fails, generation raises instead of producing a partial package.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import re
import sqlite3
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone

from . import ais

TRACK_FILENAME = "track.csv"
MANIFEST_FILENAME = "manifest.csv"
METADATA_FILENAME = "package.json"
README_FILENAME = "README.txt"

MANIFEST_COLUMNS = (
    "record",
    "csv_line",
    "role",
    "mmsi",
    "utc",
    "utc_iso",
    "station",
    "lat",
    "lon",
    "batch_id",
    "batch_filename",
    "original_line",
    "status",
    "reason",
    "detail",
)

_EPOCH_RE = re.compile(r"^-?\d+$")


class ExportError(Exception):
    """The requested package cannot be produced; nothing is downloadable."""

    def __init__(self, status: int, message: str):
        super().__init__(message)
        self.status = status
        self.message = message


@dataclass
class WindowPackage:
    data: bytes
    filename: str
    mmsi: str
    start_utc: int
    end_utc: int
    version: int
    window_rows: int
    baseline_rows: int


@dataclass
class _PayloadRow:
    row_id: int
    batch_id: str
    batch_filename: str
    original_line: int
    station: str
    utc: int
    lat: float
    lon: float
    role: str  # "baseline" | "window"
    status: str
    reason: str | None
    detail: str | None


# --------------------------------------------------------------------------
# Window bound parsing (shared by the web form and the download endpoints)
# --------------------------------------------------------------------------


def parse_utc_bound(text: str) -> int:
    """Parse an epoch second or an ISO-8601 timestamp as a UTC bound.

    Naive ISO timestamps are interpreted as UTC; explicit offsets are
    honoured.  A bare integer is a Unix epoch second.
    """

    value = (text or "").strip()
    if not value:
        raise ValueError("时间不能为空")
    if _EPOCH_RE.match(value):
        return int(value)

    candidate = value.replace("Z", "+00:00").replace("z", "+00:00")
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError as exc:
        raise ValueError(
            f"无法识别的时间 {text!r}，请用 UTC 秒或 2026-01-01T00:00:00Z 形式"
        ) from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.timestamp())


def _iso(seconds: int) -> str:
    return datetime.fromtimestamp(seconds, timezone.utc).isoformat()


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


# --------------------------------------------------------------------------
# Package construction
# --------------------------------------------------------------------------


def build_window_package(
    conn: sqlite3.Connection,
    mmsi: str,
    start_utc: int,
    end_utc: int,
    generated_at: datetime | None = None,
    max_track_bytes: int | None = None,
) -> WindowPackage:
    """Build the whole ZIP inside the caller's read snapshot.

    The caller must hold a ``read_tx`` so the vessel version, its rows and its
    verdicts all come from one published version even while uploads commit.

    ``max_track_bytes`` is the upload endpoint's size limit.  A package whose
    ``track.csv`` the reviewer could not re-upload would not be replayable, so
    such a request is refused with advice to narrow the window instead.
    """

    if start_utc >= end_utc:
        raise ExportError(400, "时间窗起点必须早于终点（含起点、不含终点）")
    if not (ais.EARLIEST_UTC <= start_utc <= ais.LATEST_UTC and
            ais.EARLIEST_UTC <= end_utc <= ais.LATEST_UTC):
        raise ExportError(400, "时间窗超出已支持的范围（2000-01-01 至 2100-01-01 UTC）")

    vessel = conn.execute(
        "SELECT mmsi, version FROM vessels WHERE mmsi = ?", (mmsi,)
    ).fetchone()
    if vessel is None:
        raise ExportError(404, "没有该船的已发布数据，无法导出时间窗")
    version = vessel["version"]

    window_rows = conn.execute(
        """
        SELECT r.id, r.batch_id, r.line_no AS original_line, r.station,
               r.utc, r.lat, r.lon,
               b.filename AS batch_filename,
               v.status, v.reason, v.detail
        FROM raw_rows r
        JOIN verdicts v
          ON v.mmsi = r.mmsi AND v.version = ? AND v.raw_row_id = r.id
        LEFT JOIN batches b ON b.id = r.batch_id
        WHERE r.mmsi = ? AND r.utc >= ? AND r.utc < ?
        ORDER BY r.id
        """,
        (version, mmsi, start_utc, end_utc),
    ).fetchall()

    expected = conn.execute(
        "SELECT COUNT(*) AS c FROM raw_rows WHERE mmsi = ? AND utc >= ? AND utc < ?",
        (mmsi, start_utc, end_utc),
    ).fetchone()["c"]
    if expected != len(window_rows):
        # Never hand out a package whose verdict list is incomplete.
        raise ExportError(
            500,
            "该版本的部分记录缺少判定，已拒绝生成可能不完整的压缩包",
        )
    if not window_rows:
        raise ExportError(
            404,
            "该 UTC 时间窗内没有这艘船的已解析记录（窗口含起点、不含终点），未生成压缩包",
        )

    baseline = conn.execute(
        """
        SELECT tp.raw_row_id AS id, r.batch_id, r.line_no AS original_line,
               r.station, r.utc, r.lat, r.lon,
               b.filename AS batch_filename
        FROM track_points tp
        JOIN raw_rows r ON r.id = tp.raw_row_id
        LEFT JOIN batches b ON b.id = r.batch_id
        WHERE tp.mmsi = ? AND tp.version = ? AND tp.utc < ?
        ORDER BY tp.utc DESC, tp.seq DESC
        LIMIT 1
        """,
        (mmsi, version, start_utc),
    ).fetchone()

    payload: list[_PayloadRow] = []
    if baseline is not None:
        payload.append(
            _PayloadRow(
                row_id=baseline["id"],
                batch_id=baseline["batch_id"],
                batch_filename=baseline["batch_filename"] or "",
                original_line=baseline["original_line"],
                station=baseline["station"],
                utc=baseline["utc"],
                lat=baseline["lat"],
                lon=baseline["lon"],
                role="baseline",
                status="trusted",
                reason=None,
                detail=None,
            )
        )
    for row in window_rows:
        payload.append(
            _PayloadRow(
                row_id=row["id"],
                batch_id=row["batch_id"],
                batch_filename=row["batch_filename"] or "",
                original_line=row["original_line"],
                station=row["station"],
                utc=row["utc"],
                lat=row["lat"],
                lon=row["lon"],
                role="window",
                status=row["status"],
                reason=row["reason"],
                detail=row["detail"],
            )
        )

    # Context row first, then the window in original arrival order.  Relative
    # order among window rows is all the replay needs: duplicate winners are
    # the earliest-arriving row of a second, and excluded rows cannot change
    # that relative order.  The baseline's UTC lies before the window, so it
    # never shares a second with a window row.
    payload.sort(key=lambda r: (0 if r.role == "baseline" else 1, r.row_id))

    track_bytes, line_numbers = _render_track_csv(payload, mmsi)
    if max_track_bytes is not None and len(track_bytes) > max_track_bytes:
        raise ExportError(
            413,
            f"该窗口的 track.csv 为 {len(track_bytes)} 字节，超过重现上传入口的 "
            f"{max_track_bytes} 字节上限，无法保证空库重演；请缩小 UTC 时间窗",
        )
    manifest_bytes = _render_manifest(payload, line_numbers, mmsi)
    self_check_replay(track_bytes, payload, line_numbers, mmsi)

    generated = generated_at or datetime.now(timezone.utc)
    metadata = {
        "format": "ais-window-export-v1",
        "vessel_mmsi": mmsi,
        "window": {
            "start_utc": start_utc,
            "end_utc": end_utc,
            "start_inclusive": True,
            "end_exclusive": True,
            "start_iso": _iso(start_utc),
            "end_iso": _iso(end_utc),
        },
        "track_version": version,
        "generated_at": generated.astimezone(timezone.utc).isoformat(timespec="seconds"),
        "max_speed_knots": ais.MAX_SPEED_KNOTS,
        "counts": {
            "window_rows": len(window_rows),
            "baseline_rows": 1 if baseline is not None else 0,
            "payload_rows": len(payload),
            "trusted": sum(1 for r in window_rows if r["status"] == "trusted"),
            "isolated": sum(1 for r in window_rows if r["status"] == "isolated"),
        },
        "baseline": None
        if baseline is None
        else {
            "utc": baseline["utc"],
            "utc_iso": _iso(baseline["utc"]),
            "batch_id": baseline["batch_id"],
            "original_line": baseline["original_line"],
            "station": baseline["station"],
        },
        "files": {
            TRACK_FILENAME: {
                "sha256": _sha256(track_bytes),
                "bytes": len(track_bytes),
            },
            MANIFEST_FILENAME: {
                "sha256": _sha256(manifest_bytes),
                "bytes": len(manifest_bytes),
            },
        },
        "replay": {
            "endpoint": "POST /upload (multipart form field 'file')",
            "file": TRACK_FILENAME,
            "expected_vessel_mmsi": mmsi,
            "expected_window_rows": len(window_rows),
        },
    }
    metadata_bytes = (
        json.dumps(metadata, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    ).encode("utf-8")
    readme_bytes = _render_readme(mmsi, start_utc, end_utc, version, metadata).encode(
        "utf-8"
    )

    buffer = io.BytesIO()
    try:
        with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
            archive.writestr(TRACK_FILENAME, track_bytes)
            archive.writestr(MANIFEST_FILENAME, manifest_bytes)
            archive.writestr(METADATA_FILENAME, metadata_bytes)
            archive.writestr(README_FILENAME, readme_bytes)
    except Exception as exc:  # noqa: BLE001 - convert to a clean refusal
        raise ExportError(500, f"压缩包生成失败，未提供任何下载: {exc}") from exc

    data = buffer.getvalue()
    return WindowPackage(
        data=data,
        filename=f"ais_window_{mmsi}_{start_utc}_{end_utc}.zip",
        mmsi=mmsi,
        start_utc=start_utc,
        end_utc=end_utc,
        version=version,
        window_rows=len(window_rows),
        baseline_rows=1 if baseline is not None else 0,
    )


def self_check_replay(
    track_bytes: bytes,
    payload: list[_PayloadRow],
    line_numbers: dict[int, int],
    mmsi: str,
) -> None:
    """Re-parse the exported CSV and replay it before anything is downloadable.

    The reviewer's import goes through the real upload parser, so this check
    does too: float formatting, quoting, line numbers and row order must all
    survive a round trip.  Row ids are reassigned 1..n in file order, mirroring
    what a fresh upload does.  Any difference - status, reason or detail -
    aborts the export so a package that cannot be replayed is never offered.
    """

    try:
        outcome = ais.parse_ais_csv(track_bytes)
    except ais.CsvFormatError as exc:
        raise ExportError(500, f"导出校验失败：包内 CSV 无法被上传入口解析: {exc}") from exc
    except Exception as exc:  # noqa: BLE001 - csv.Error and friends
        raise ExportError(
            500, f"导出校验失败：包内 CSV 无法被上传入口解析: {exc}"
        ) from exc
    # MMSIs are stored without leading zeros; export and re-import agree.
    if outcome.rejected or len(outcome.rows) != len(payload):
        raise ExportError(500, "导出校验失败：包内 CSV 行数与预期不符或被拒绝")
    if any(parsed.mmsi != mmsi for parsed in outcome.rows):
        raise ExportError(500, "导出校验失败：包内 CSV 混入了其他船舶的记录")

    # The parser reports physical line numbers; they must be the ones the
    # manifest promises to the reviewer.
    for parsed, row in zip(outcome.rows, payload):
        if parsed.line_no != line_numbers[row.row_id]:
            raise ExportError(
                500,
                "导出校验失败：对照清单的行号与包内 CSV 不一致"
                f"（预期第 {line_numbers[row.row_id]} 行，实为第 {parsed.line_no} 行）",
            )

    candidates = [
        ais.Candidate(
            row_id=index + 1,
            batch_id=row.batch_id,
            station=parsed.station,
            utc=parsed.utc,
            lat=parsed.lat,
            lon=parsed.lon,
        )
        for index, (row, parsed) in enumerate(zip(payload, outcome.rows))
    ]
    evaluation = ais.evaluate_track(candidates)
    for index, row in enumerate(payload):
        if row.role != "window":
            continue
        replayed = evaluation.candidate_status.get(index + 1)
        expected = (row.status, row.reason, row.detail)
        if replayed != expected:
            raise ExportError(
                500,
                "导出校验失败：窗口判定无法由裁剪后的记录重演，已拒绝生成压缩包"
                f"（UTC {row.utc}，{row.batch_filename} 第 {row.original_line} 行）",
            )


def _render_track_csv(
    payload: list[_PayloadRow], mmsi: str
) -> tuple[bytes, dict[int, int]]:
    """Write the replay CSV and remember the physical line of each record.

    The upload parser reports ``csv.reader.line_num`` (the last physical line
    of a record), so quoted newlines are accounted for correctly.
    """

    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(("station", "mmsi", "utc", "lat", "lon"))
    line_numbers: dict[int, int] = {}
    consumed = 1  # header line
    for row in payload:
        # Render one record at a time; counting newlines on the whole buffer
        # on every iteration would make large windows quadratic.
        record = io.StringIO()
        csv.writer(record, lineterminator="\n").writerow(
            (
                row.station,
                mmsi,
                row.utc,
                f"{row.lat:.6f}",
                f"{row.lon:.6f}",
            )
        )
        chunk = record.getvalue()
        buffer.write(chunk)
        consumed += chunk.count("\n")
        line_numbers[row.row_id] = consumed
    return buffer.getvalue().encode("utf-8"), line_numbers


def _render_manifest(
    payload: list[_PayloadRow], lines: dict[int, int], mmsi: str
) -> bytes:
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(MANIFEST_COLUMNS)
    # Chronological for review; same-second rows keep arrival order, and the
    # csv_line column always points at the real file order.
    ordered = sorted(enumerate(payload), key=lambda item: (item[1].utc, item[0]))
    for record, (_, row) in enumerate(ordered, start=1):
        writer.writerow(
            (
                record,
                lines[row.row_id],
                row.role,
                mmsi,
                row.utc,
                _iso(row.utc),
                row.station,
                f"{row.lat:.6f}",
                f"{row.lon:.6f}",
                row.batch_id,
                row.batch_filename,
                row.original_line,
                row.status,
                row.reason or "",
                row.detail or "",
            )
        )
    return buffer.getvalue().encode("utf-8")


def _render_readme(
    mmsi: str, start_utc: int, end_utc: int, version: int, metadata: dict
) -> str:
    baseline = metadata["baseline"]
    baseline_text = (
        "无（窗口前没有可信点，首条窗口记录是重演的起点）"
        if baseline is None
        else (
            f"UTC {baseline['utc']}（{baseline['utc_iso']}），"
            f"批次 {baseline['batch_id']} 原档第 {baseline['original_line']} 行"
        )
    )
    return f"""AIS 可信航迹时间窗导出（{metadata['format']}）

船舶 MMSI：{mmsi}
导出船舶版本：v{version}
时间窗：UTC {start_utc} ≤ t < {end_utc}
       {_iso(start_utc)} ≤ t < {_iso(end_utc)}（含起点、不含终点）
窗前可信基准：{baseline_text}
窗口记录数：{metadata['counts']['window_rows']}（可信 {metadata['counts']['trusted']}，隔离 {metadata['counts']['isolated']}）
速度门限：{metadata['max_speed_knots']:.0f} 节（相对上一可信点）

文件
----
{TRACK_FILENAME}
    单船 CSV，可直接经网页“上传”入口（POST /upload，字段名 file）导入空库重演。
    包含窗口内全部已解析记录，外加窗口前最后一个可信点（如有）。
    行序为原始到达顺序；同秒重投、冲突与先到次序均保持原样。
{MANIFEST_FILENAME}
    窗口内每条记录的判定对照清单：在本 CSV 中的行号（csv_line）、所属原批次
    （batch_id / batch_filename）与原档行号（original_line），以及可信/隔离结论
    与原因。role=baseline 的行为窗口前的可信基准，不属于窗口判定清单。
{METADATA_FILENAME}
    机器可读的窗口、版本与文件校验信息（sha256）。
{README_FILENAME}
    本说明。

重演校验
--------
在空库上传 {TRACK_FILENAME} 后，查看该船 {mmsi} 的判定：窗口内每条记录
（manifest 中 role=window 的行）的 status / reason / detail 应与清单完全一致。
窗口前的基准行只提供速度门限的起点，不计入窗口结论。

注意
----
* 清单反映导出时刻的已发布版本 v{version}；后续上传新的原档会重算该船，结论可能变化。
* 解析拒绝行从未参与判定，因此不在包内。
"""


