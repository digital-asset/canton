import importlib.util
from pathlib import Path


def _load_prepare_rerun_module():
    module_path = Path(__file__).with_name("prepare_rerun_command.py")
    spec = importlib.util.spec_from_file_location("prepare_rerun_command", module_path)
    assert spec is not None and spec.loader is not None, (
        f"Could not load module spec for {module_path}"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


prepare_rerun_command = _load_prepare_rerun_module()
build_resolved_command = prepare_rerun_command.build_resolved_command
select_rerun_classes = prepare_rerun_command.select_rerun_classes


def test_select_rerun_classes_includes_failed_and_unexecuted_when_timeout_triggered():
    rerun = select_rerun_classes(
        failed_classes=["com.example.Failed", "com.example.Failed"],
        selected_classes=["com.example.Failed", "com.example.NotExecuted"],
        executed_classes={"com.example.Failed"},
        testcase_timeout_triggered=True,
    )
    assert rerun == ["com.example.Failed", "com.example.NotExecuted"]


def test_select_rerun_classes_falls_back_to_full_selected_when_junit_incomplete():
    rerun = select_rerun_classes(
        failed_classes=[],
        selected_classes=["com.example.A", "com.example.B"],
        executed_classes=set(),
        testcase_timeout_triggered=True,
    )
    assert rerun == ["com.example.A", "com.example.B"]


def test_select_rerun_classes_does_not_force_full_rerun_when_all_selected_look_executed():
    rerun = select_rerun_classes(
        failed_classes=[],
        selected_classes=["com.example.A", "com.example.B"],
        executed_classes={"com.example.A", "com.example.B"},
        testcase_timeout_triggered=True,
    )
    assert rerun == []


def test_build_resolved_command_in_non_sharded_mode_is_quoted():
    resolved = build_resolved_command(
        command_template="placeholder",
        test_sub_command="testOnly",
        rerun_classes=["com.example.Foo"],
        num_test_buckets=0,
    )
    assert resolved == '"testOnly com.example.Foo"'


def test_build_resolved_command_replaces_split_placeholder_when_sharded():
    resolved = build_resolved_command(
        command_template='dumpClassPath "$RUN_SPLITTED_TESTS_CMD" checkErrors',
        test_sub_command="testOnly",
        rerun_classes=["com.example.Foo", "com.example.Bar"],
        num_test_buckets=4,
    )
    assert resolved == 'dumpClassPath "testOnly com.example.Foo com.example.Bar" checkErrors'
