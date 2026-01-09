
import sys
from pathlib import Path

# Add project root to sys.path so pytest can find src module
PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))
