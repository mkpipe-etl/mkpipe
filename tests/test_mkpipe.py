import os
from mkpipe import run

os.chdir(os.path.dirname(os.path.abspath(__file__)))
config_file = 'elt.yaml'
run(config_file)
