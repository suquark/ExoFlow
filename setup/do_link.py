import importlib
import os

setup_dev = importlib.import_module("ray.setup-dev")

# chdir because 'do_link' is relative to the current directory
os.chdir(os.path.expanduser("~/ExoFlow/patch"))

setup_dev.do_link("_private/storage.py", force=True)
setup_dev.do_link("dag", force=True)
