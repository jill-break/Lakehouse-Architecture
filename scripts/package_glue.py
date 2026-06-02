"""Package glue_jobs/common/ into glue_jobs/dist/common.zip for deployment."""

import os
import zipfile

os.makedirs("glue_jobs/dist", exist_ok=True)

files = {
    "glue_jobs/common/__init__.py": "common/__init__.py",
    "glue_jobs/common/utils.py": "common/utils.py",
}

with zipfile.ZipFile("glue_jobs/dist/common.zip", "w") as z:
    for src, arcname in files.items():
        if os.path.exists(src):
            z.write(src, arcname)
        else:
            # Create empty file in zip if missing (e.g. __init__.py)
            z.writestr(arcname, "")
        print(f"  added {arcname}")

print("Packaged glue_jobs/dist/common.zip")
