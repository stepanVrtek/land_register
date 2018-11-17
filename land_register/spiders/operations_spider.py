import scrapy
from urllib.parse import urljoin
from datetime import datetime
from land_register import db_handler
from pprint import pprint


BASE_URL = 'https://nahlizenidokn.cuzk.cz/'
START_URL = 'https://nahlizenidokn.cuzk.cz/VyberRizeni.aspx'

OUTPUT_TYPE = 'ctl00$bodyPlaceHolder$typVystupu'
LIST_TYPE = 'SeznamDatum'

OPERATION_TYPE = 'ctl00$bodyPlaceHolder$listTypRizeni'
WORKPLACE = 'ctl00$bodyPlaceHolder$vyberPracoviste$listPracoviste'

DATE_INPUT = 'ctl00$bodyPlaceHolder$txtDatum'

SEARCH_BTN = 'ctl00$bodyPlaceHolder$btnVyhledat'
SEARCH_BTN_TXT = 'Vyhledat'


class OperationsSpider(scrapy.Spider):
    """Spider of all operations (rizeni) in workplace (katastralni pracoviste)
    and with selected type and date."""

    name = "OperationsSpider"
    start_urls = [START_URL]
    custom_settings = {
        'ITEM_PIPELINES': {
            'land_register.pipelines.operations_pipeline.OperationsPipeline': 200
        }
    }

    def __init__(self, workplace, type, job_id=None, date=None, **kwargs):
        self.workplace = int(workplace)
        self.type = type
        self.job_id = job_id
        self.date = date if date else self.today()

        # we consider spider as finished, doesn't matter about result
        # there are lot of processes, that can add missing items in next runs
        if self.job_id:
            self.update_rizeni_log(status='F')

        super().__init__(**kwargs)


    def today(self):
        return datetime.now().strftime('%d.%m.%Y')


    def response_is_ban(self, request, response):
        return response.status == 403 or response.status == 500


    def update_rizeni_log(self, status):
        db = db_handler.get_dataset()
        db['log_rizeni'].update(dict(
            id=self.job_id,
            stav=status,
            datum_konce=datetime.now()
        ), ['id'])


    def parse(self, response):
        """Select 'Seznam podle rizeni' radio button."""

        yield scrapy.FormRequest.from_response(
            response,
            formdata = {
                OUTPUT_TYPE: LIST_TYPE
            },
            callback = self.open_operations_lists
        )


    def open_operations_lists(self, response):
        """Open lists of operations for input WP and type."""

        yield scrapy.FormRequest.from_response(
            response,
            meta = {
                'cislo_pracoviste': self.workplace
            },
            formdata = {
                OPERATION_TYPE: self.type,
                DATE_INPUT : self.date,
                WORKPLACE: str(self.workplace),
                SEARCH_BTN: SEARCH_BTN_TXT
            },
            callback = self.process_operations_list
        )


    def process_operations_list(self, response):
        """Process all operations items."""

        operations_list = response.xpath(
            '//table[@summary]/tbody/tr')
        for row in operations_list:
            number = row.xpath('td[1]/a/text()').extract_first()
            if number is None:
                continue

            state = row.xpath('td[3]/a/text()').extract_first()
            wp = response.meta['cislo_pracoviste']

            # check current state of operation - if is updated, process next
            if is_operation_updated(number, wp, state):
                continue

            ref = row.xpath('td[1]/a/@href').extract_first()
            url = urljoin(BASE_URL, ref)

            yield scrapy.Request(
                url,
                meta = response.meta,
                callback = self.parse_operation
            )


    def parse_operation(self, response):
        """Parse and save operation item."""

        if is_error_message(response):
            return

        operation_item = {
            'item_type': 'RIZENI',
            'data': {}
        }
        operation_data = {
            'cislo_pracoviste': response.meta['cislo_pracoviste']
        }

        # atributy
        operation_table = response.xpath(
            '//table[@summary="Atributy řízení"]/tr')
        for row in operation_table:
            name = row.xpath('td[1]/text()').extract_first()
            value = row.xpath('td[2]/text()').extract_first()
            if not name:
                name = row.xpath('td[1]/strong/text()').extract_first()
                value = row.xpath('td[2]/strong/text()').extract_first()

            name = {
                'Číslo řízení:': 'cislo_rizeni',
                'Datum přijetí:': 'datum_prijeti',
                'Stav řízení:': 'stav_rizeni'
            }.get(name)

            if name:
                operation_data[name] = value

        ku_string = response.xpath(
            '//p[contains(text(), "Řízení se týká nemovitostí v k.ú.")]'
            ).extract_first()
        if ku_string:
            ku_code = get_string_between_brackets(ku_string)
            operation_data['cislo_ku'] = ku_code

        operation_item['data'] = operation_data


        # ------------------------------------------------------------------
        # ucastnici rizeni
        operation_item['ucastnici'] = []
        participants_table = response.xpath(
            '//table[@summary="Účastníci řízení"]/tbody/tr')
        for index, row in enumerate(participants_table):
            participant = {
                'poradove_cislo': index + 1,
                'jmeno': row.xpath('td[1]/text()').extract_first(),
                'typ': row.xpath('td[2]/text()').extract_first()
            }
            operation_item['ucastnici'].append(participant)


        # ------------------------------------------------------------------
        # provedene operace
        operation_item['provedene_operace'] = []
        performed_ops_table = response.xpath(
            '//table[@summary="Provedené operace"]/tbody/tr')
        for index, row in enumerate(performed_ops_table):
            performed_op = {
                'poradove_cislo': index + 1,
                'operace': row.xpath('td[1]/a/text()').extract_first(),
                'datum': row.xpath('td[2]/text()').extract_first()
            }
            operation_item['provedene_operace'].append(performed_op)


        # ------------------------------------------------------------------
        # predmety rizeni
        operation_item['predmety_rizeni'] = []
        operations_subjects_table = response.xpath(
            '//table[@summary="Předměty řízení"]/tbody/tr')
        for index, row in enumerate(operations_subjects_table):
            subject = {
                'poradove_cislo': index + 1,
                'typ': row.xpath('td[1]/text()').extract_first()
            }
            operation_item['predmety_rizeni'].append(subject)


        # ------------------------------------------------------------------
        # seznam nemovitosti
        operation_item['seznam_nemovitosti'] = []
        property_list_table = response.xpath(
            '//table[@summary="Seznam nemovitostí, ke kterým byl v rámci řízení zapsán cenový údaj"]/tbody/tr')
        index = 1
        ref_requests = []
        for row in property_list_table:
            type_name = row.xpath('th[1]/text()').extract_first()
            if type_name:
                type = {
                    'Parcely': 'PARCELA',
                    'Jednotky': 'JEDNOTKA'
                }.get(type_name, 'NEZNAMY')
                continue

            property = {
                'poradove_cislo': index,
                'typ': type,
                'cislo': row.xpath('td[1]/a/text()').extract_first()
            }

            ref = row.xpath('td/a/@href').extract_first()
            url = urljoin(BASE_URL, ref)

            meta = {
                'cislo_rizeni': operation_item['cislo_rizeni'],
                'cislo_pracoviste': operation_item['cislo_pracoviste']
            }

            if type == 'PARCELA':
                ref_requests.append(
                    scrapy.Request(
                        url,
                        meta = meta,
                        callback = self.parse_ground_refs
                    )
                )
            elif type == 'JEDNOTKA':
                ref_requests.append(
                    scrapy.Request(
                        url,
                        meta = meta,
                        callback = self.parse_unit_refs
                    )
                )

            operation_item['seznam_nemovitosti'].append(property)
            index += 1


        # finally yield operation item and reference items
        yield operation_item

        for r in ref_requests:
            yield r


    def parse_ground_refs(self, response):
        """Parse LV references from ground - for operation purposes."""

        ground_ref_item = {
            'item_type': 'REF_PARCELA_RIZENI',
            'cislo_rizeni': response.meta['cislo_rizeni'],
            'cislo_pracoviste': response.meta['cislo_pracoviste']
        }

        ground_table = response.xpath(
            '//table[@summary="Atributy parcely"]/tr')
        for row in ground_table:
            name = row.xpath('td[1]/text()').extract_first()

            name = {
                'Parcelní číslo:': 'parcelni_cislo',
                'Katastrální území:': 'cislo_ku',
                'Číslo LV:': 'cislo_lv'
            }.get(name)

            if name:
                value = row.xpath('td[2]/a/text()').extract_first()
                ground_ref_item[name] = value

        if ground_ref_item.get('cislo_ku'):
            _, ground_ref_item['cislo_ku'] = parse_string_w_num(
                ground_ref_item['cislo_ku'])

        ground_ref_item['lv_item'] = {
            'cislo_lv': ground_ref_item.get('cislo_lv'),
            'cislo_ku': ground_ref_item.get('cislo_ku')
        }

        yield ground_ref_item


    def parse_unit_refs(self, response):
        """Parse LV references from unit - for operation purposes."""

        unit_ref_item = {
            'item_type': 'REF_JEDNOTKA_RIZENI',
            'cislo_rizeni': response.meta['cislo_rizeni'],
            'cislo_pracoviste': response.meta['cislo_pracoviste']
        }

        unit_table = response.xpath(
            '//table[@summary="Atributy jednotky"]/tr')
        for row in unit_table:
            name = row.xpath('td[1]/text()').extract_first()
            value = None
            if not name:
                name = row.xpath('td[1]/strong/text()').extract_first()
                value = row.xpath('td[2]/strong/text()').extract_first()

            name = {
                'Číslo jednotky': 'cislo_jednotky',
                'Katastrální území:': 'cislo_ku',
                'Číslo LV:': 'cislo_lv'
            }.get(name)

            if not name:
                continue

            if not value:
                value = row.xpath('td[2]/a/text()').extract_first()

            unit_ref_item[name] = value

        if unit_ref_item.get('cislo_ku'):
            _, unit_ref_item['cislo_ku'] = parse_string_w_num(
                unit_ref_item['cislo_ku'])

        unit_ref_item['lv_item'] = {
            'cislo_lv': unit_ref_item.get('cislo_lv'),
            'cislo_ku': unit_ref_item.get('cislo_ku')
        }

        yield unit_ref_item



def is_error_message(response):
    """Checks if error message appeared on a page."""

    error_message = response.xpath(
        '//div[@id="ctl00_hlaseniOnMasterPage"]/span/text()').extract_first()

    if error_message and error_message != 'Zadaný LV nebyl nalezen!':
        print(error_message)
    return error_message is not None


def is_operation_updated(operation_number, wp, state):
    """Checks if operation has been updated already."""

    db = db_handler.get_dataset()
    result = db['rizeni'].find_one(
        cislo_rizeni=operation_number,
        cislo_pracoviste=wp,
        stav_rizeni=state
    )
    return True if result else False


def get_string_between_brackets(string):
    return string[string.find("(")+1:string.find(")")]


def parse_string_w_num(input):
    num = input[input.find('[') + 1:input.find(']')]
    string = input.replace('[' + num + ']', '').strip()
    return (string, num)
