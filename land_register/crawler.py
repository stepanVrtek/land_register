import spiders
from scrapyd_api import ScrapydAPI
from datetime import datetime, timedelta

def get_scrapyd():
    return ScrapydAPI('http://localhost:6800')


class LandRegisterCrawler():
    _project_name = 'land_register'


class OperationCrawler(LandRegisterCrawler):
    _spider_name = 'OperationsSpider'
    _workplaces = [
        20, 101, 301, 302, 303, 305, 306, 307, 308, 701, 731, 702, 703, 704,
        706, 735, 738, 712, 713, 402, 403, 409, 602, 604, 605, 607, 610, 501,
        532, 504, 505, 608, 801, 802, 803, 831, 804, 806, 807, 832, 835, 811,
        805, 709, 808, 809, 603, 606, 609, 611, 401, 404, 435, 406, 405, 407,
        408, 410, 201, 202, 203, 204, 205, 206, 207, 208, 209, 210, 211, 212,
        231, 502, 503, 506, 507, 508, 531, 509, 510, 533, 601, 707, 741, 304,
        710, 746, 714, 740, 708, 711, 742, 737, 836, 810, 705
    ]
    _operation_types = ['V', 'Z']

    @staticmethod
    def run():
        scrapyd = get_scrapyd()

        start_date = datetime(2018, 9, 24)
        end_date = datetime.now()
        date = end_date

        while date >= start_date:
            formatted_date = date.strftime('%d.%m.%Y')

            for type in OperationCrawler._operation_types:
                for wp in OperationCrawler._workplaces:
                    scrapyd.schedule(
                        OperationCrawler._project_name,
                        OperationCrawler._spider_name,
                        workplace = str(wp),
                        type = type,
                        date = formatted_date
                    )

            date -= timedelta(days = 1)

if __name__ == '__main__':
    OperationCrawler.run()
