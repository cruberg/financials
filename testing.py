import pandas as pd
import numpy as np
from  alpaca import Universe

universe = Universe()

prices = universe.get_universe_prices(252)