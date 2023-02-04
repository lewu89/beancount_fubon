import sys
from os import path
sys.path.insert(0, path.join(path.dirname(__file__)))

import run

CONFIG = [
  run.Importer("Liabilities:Fubon")
]