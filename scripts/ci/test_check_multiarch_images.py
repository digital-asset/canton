import importlib.util
from pathlib import Path


def _load_module():
    module_path = Path(__file__).with_name("check-multiarch-images.py")
    spec = importlib.util.spec_from_file_location("check_multiarch_images", module_path)
    assert spec is not None and spec.loader is not None, (
        f"Could not load module spec for {module_path}"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


mod = _load_module()


def _entry(os_name: str, arch: str, attestation: bool = False) -> dict:
    entry: dict = {"platform": {"os": os_name, "architecture": arch}}
    if attestation:
        entry["annotations"] = {"vnd.docker.reference.type": "attestation-manifest"}
    return entry


def test_accepts_manifest_with_amd64_and_arm64_and_attestations():
    manifest = {
        "manifests": [
            _entry("linux", "amd64"),
            _entry("linux", "arm64"),
            _entry("unknown", "unknown", attestation=True),
        ]
    }
    assert mod._is_multi_arch_manifest(manifest) is True


def test_rejects_single_arch_index_with_attestation():
    manifest = {
        "manifests": [
            _entry("linux", "amd64"),
            _entry("unknown", "unknown", attestation=True),
        ]
    }
    assert mod._is_multi_arch_manifest(manifest) is False


def test_rejects_single_arch_manifest_without_index():
    assert mod._is_multi_arch_manifest({"config": {}, "layers": []}) is False


def test_main_requires_at_least_one_reference():
    assert mod.main([]) == 2


def test_main_passes_when_all_inspected_refs_succeed(monkeypatch):
    monkeypatch.setattr(mod, "_inspect", lambda ref: True)
    assert mod.main(["reg/img:1", "reg/other:1"]) == 0


def test_main_fails_when_any_inspected_ref_fails(monkeypatch):
    monkeypatch.setattr(mod, "_inspect", lambda ref: ref == "reg/ok:1")
    assert mod.main(["reg/ok:1", "reg/bad:1"]) == 1
