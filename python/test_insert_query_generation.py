import pandas as pd
import numpy as np
from db_query import Databases


x = np.random.randint(0,10,(18,2))
x = pd.DataFrame(x)

db = Databases()

db.test_insert_string(x, 'schema', 'table')