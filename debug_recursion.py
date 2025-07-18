# %%
import pickle

from traffic.data import airports

apt = airports["ZRH"]
p = pickle.dumps(apt)
apt = pickle.loads(p)

# %%
