"""Tests for deterministic generation and manifest verification."""
import filecmp
import shutil
import tempfile
import unittest
from pathlib import Path

from reconciliation.generate import generate_batch, calculate_sha256


class TestGeneratorDeterminism(unittest.TestCase):
    def setUp(self):
        self.test_dir = Path(tempfile.mkdtemp())

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_identical_seed_produces_identical_files(self):
        dir1 = self.test_dir / "run1"
        dir2 = self.test_dir / "run2"

        m1 = generate_batch(output_dir=dir1, scenario="baseline", seed=100, base_time="2026-01-01T00:00:00Z")
        m2 = generate_batch(output_dir=dir2, scenario="baseline", seed=100, base_time="2026-01-01T00:00:00Z")

        self.assertEqual(m1["batch_id"], m2["batch_id"])
        self.assertEqual(len(m1["files"]), len(m2["files"]))

        # Check each file's exact hash equality
        for f1, f2 in zip(m1["files"], m2["files"]):
            self.assertEqual(f1["filename"], f2["filename"])
            self.assertEqual(f1["sha256_hex"], f2["sha256_hex"])
            p1 = dir1 / f1["filename"]
            p2 = dir2 / f2["filename"]
            self.assertTrue(filecmp.cmp(p1, p2, shallow=False))

    def test_manifest_hashes_match_disk_bytes(self):
        out = self.test_dir / "batch"
        manifest = generate_batch(output_dir=out, scenario="baseline", seed=42)

        for entry in manifest["files"]:
            target_file = out / entry["filename"]
            self.assertTrue(target_file.is_file())
            actual_hash = calculate_sha256(target_file)
            self.assertEqual(entry["sha256_hex"], actual_hash, f"Hash mismatch on {entry['filename']}")


if __name__ == "__main__":
    unittest.main()
