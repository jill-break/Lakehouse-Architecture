"""Package glue_jobs/common/ into dist/common.zip for deployment."""

import os
import zipfile

os.makedirs("glue_jobs/dist", exist_ok=True)

with zipfile.ZipFile("glue_jobs/dist/common.zip", "w") as z:
    z.write("glue_jobs/common/__init__.py", "common/__init__.py")
    z.write("glue_jobs/common/utils.py", "common/utils.py")

print("Packaged glue_jobs/dist/common.zip")
