import importlib.util
from pathlib import Path


def _load_module():
    module_path = Path(__file__).with_name("join_multiline_log_records.py")
    spec = importlib.util.spec_from_file_location("join_multiline_log_records", module_path)
    assert spec is not None and spec.loader is not None, (
        f"Could not load module spec for {module_path}"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


join_multiline_log_records = _load_module()
collapse_log_records = join_multiline_log_records.collapse_log_records


def test_collapse_log_records_merges_indented_benchmark_continuation_lines():
    records = list(
        collapse_log_records(
            [
                "WARN  c.d.c.s.c.PeriodicAcknowledgements:MediatorReplayBenchmark/foo - Failed to acknowledge clean timestamp (usually because sequencer is down): ConnectionError(TransportError(Request failed for server-sequencer1-0.\n",
                "  GrpcClientGaveUp: CANCELLED/RST_STREAM closed stream. HTTP/2 error code: CANCEL\n",
                "  Request: acknowledge-signed/2026-08-27T23:18:45.408584Z))\n",
                "[info] All tests passed.\n",
            ]
        )
    )

    assert records == [
        "WARN  c.d.c.s.c.PeriodicAcknowledgements:MediatorReplayBenchmark/foo - Failed to acknowledge clean timestamp (usually because sequencer is down): ConnectionError(TransportError(Request failed for server-sequencer1-0. GrpcClientGaveUp: CANCELLED/RST_STREAM closed stream. HTTP/2 error code: CANCEL Request: acknowledge-signed/2026-08-27T23:18:45.408584Z))",
        "[info] All tests passed.",
    ]


def test_collapse_log_records_keeps_separate_non_indented_records():
    records = list(
        collapse_log_records(
            [
                "WARN  first warning\n",
                "ERROR second record\n",
            ]
        )
    )

    assert records == [
        "WARN  first warning",
        "ERROR second record",
    ]


def test_collapse_log_records_flushes_on_blank_lines():
    records = list(
        collapse_log_records(
            [
                "WARN  first warning\n",
                "  detail line\n",
                "\n",
                "ERROR second record\n",
            ]
        )
    )

    assert records == [
        "WARN  first warning",
        "  detail line",
        "ERROR second record",
    ]


def test_collapse_log_records_does_not_join_indented_lines_after_non_log_headers():
    records = list(
        collapse_log_records(
            [
                "plain heading\n",
                "  indented payload\n",
                "WARN  real warning\n",
                "  warning details\n",
            ]
        )
    )

    assert records == [
        "plain heading",
        "  indented payload",
        "WARN  real warning",
        "  warning details",
    ]


def test_collapse_log_records_only_joins_benchmark_scoped_records():
    records = list(
        collapse_log_records(
            [
                "WARN  c.d.c.s.c.PeriodicAcknowledgements:SomeOtherTest/foo - Failed to acknowledge clean timestamp\n",
                "  GrpcClientGaveUp: CANCELLED/RST_STREAM closed stream. HTTP/2 error code: CANCEL\n",
                "WARN  c.d.c.s.c.PeriodicAcknowledgements:MediatorReplayBenchmark/foo - Failed to acknowledge clean timestamp\n",
                "  GrpcClientGaveUp: CANCELLED/RST_STREAM closed stream. HTTP/2 error code: CANCEL\n",
            ]
        )
    )

    assert records == [
        "WARN  c.d.c.s.c.PeriodicAcknowledgements:SomeOtherTest/foo - Failed to acknowledge clean timestamp",
        "  GrpcClientGaveUp: CANCELLED/RST_STREAM closed stream. HTTP/2 error code: CANCEL",
        "WARN  c.d.c.s.c.PeriodicAcknowledgements:MediatorReplayBenchmark/foo - Failed to acknowledge clean timestamp GrpcClientGaveUp: CANCELLED/RST_STREAM closed stream. HTTP/2 error code: CANCEL",
    ]


def test_collapse_log_records_does_not_append_indented_new_record_start():
    records = list(
        collapse_log_records(
            [
                "WARN  c.d.c.s.c.PeriodicAcknowledgements:MediatorReplayBenchmark/foo - Failed to acknowledge clean timestamp\n",
                "  ERROR distinct follow-up record\n",
            ]
        )
    )

    assert records == [
        "WARN  c.d.c.s.c.PeriodicAcknowledgements:MediatorReplayBenchmark/foo - Failed to acknowledge clean timestamp",
        "  ERROR distinct follow-up record",
    ]
