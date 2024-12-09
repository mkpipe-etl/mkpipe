import os
from mkpipe.run import main

os.chdir(os.path.dirname(os.path.abspath(__file__)))
# config_file = 'mkpipe_project.yaml'
# run(config_file)
main()
