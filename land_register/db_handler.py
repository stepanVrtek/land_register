import dataset
from scrapy.utils.project import get_project_settings


def get_dataset():
    settings = get_project_settings()
    return dataset.connect(settings['DB_CONNECTION'])