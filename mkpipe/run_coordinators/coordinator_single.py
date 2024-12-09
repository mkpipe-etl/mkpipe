import datetime
from ..plugins import get_extractor, get_loader
from ..utils import Logger

logger = Logger(__file__)


class CoordinatorSingle:
    def __init__(self, task_group):
        self.task_group = task_group

    def load_data(self, task):
        # Initialize loader instance
        loader = get_loader(task.loader_variant)(task.loader_conf, task.settings)

        # Record the start time of the loading process
        elt_start_time = datetime.datetime.now()

        # Load the extracted data with the start time
        loader.load(task.data, elt_start_time)

        logger.info({'message': 'Loaded data successfully!'})
        return True  # Return True to indicate success

    def extract_data(self, task):
        # Initialize extractor and loader instances
        extractor = get_extractor(task.extractor_variant)(
            task.current_table_conf, task.settings
        )

        # Perform the data extraction
        task.data = extractor.extract()
        logger.info({'message': 'Extracted data successfully!'})

        if task.data:
            # Schedule the data loading as a separate task
            self.load_data(task)

        return True  # Return True to indicate success

    def run(self):
        for task in self.task_group:
            self.extract_data(task)
