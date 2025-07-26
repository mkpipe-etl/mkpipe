import datetime
from ..plugins import get_extractor, get_loader
from ..utils import Logger


class CoordinatorSingle:
    def __init__(self, task_group):
        self.task_group = task_group
        self.logger = Logger(__file__)

    def extract_load(self, task):
        extractor = get_extractor(task.extractor_variant)(task.table_extract_conf, task.settings)
        task.data = extractor.extract()

        if task.data:
            loader = get_loader(task.loader_variant)(task.table_load_conf, task.settings)
            elt_start_time = datetime.datetime.now()
            loader.load(task.data, elt_start_time)

        self.logger.info({'message': 'Extract-Load is successfull!'})
        return True

    def run(self):
        for task in self.task_group:
            self.extract_load(task)
