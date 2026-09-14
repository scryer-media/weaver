"""Regenerate unmodified embedded bytes with the pinned official PAR3 binary."""
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import zipfile

root = Path(__file__).resolve().parent
reference = Path(sys.argv[1]).resolve()
archive = root / "archive.zip"
payload = bytes((i * 17 + (i >> 8) * 13 + 7) & 255 for i in range(8192))
member = zipfile.ZipInfo("payload.bin", (2020, 1, 1, 0, 0, 0))
member.compress_type = zipfile.ZIP_STORED
with zipfile.ZipFile(archive, "w") as output:
    output.writestr(member, payload)
original = archive.read_bytes()
args = ["insert", archive.name]
result = subprocess.run([str(reference), *args], cwd=root, capture_output=True, check=True)
(root / "insertion.txt").write_bytes(result.stdout + result.stderr)
inserted = archive.read_bytes()
assert inserted.startswith(original)
digest = lambda data: hashlib.sha256(data).hexdigest()
(root / "provenance.json").write_text(json.dumps({
    "referenceRevision": "2971702e501f1350b1c7b9d11369af9157d6ed56",
    "referenceSHA256": digest(reference.read_bytes()),
    "arguments": args,
    "payloadSHA256": digest(payload),
    "originalSHA256": digest(original),
    "insertedSHA256": digest(inserted),
    "protectedLength": len(original),
}, indent=2) + "\n")
