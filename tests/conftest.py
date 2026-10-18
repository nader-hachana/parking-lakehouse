import sys
from pathlib import Path

# The generator scripts are plain files in generator/, not an installed package.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "generator"))
