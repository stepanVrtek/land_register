import scrapy
from urllib.parse import urljoin
from pprint import pprint
from datetime import datetime

import mysql.connector as mariadb


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
    name = "OperationsSpider"
    start_urls = [START_URL]

    def __init__(self, date=None, workplace=None, **kwargs):
        if date is None:
            self.date = datetime.now().strftime('%d.%m.%Y')
        else:
            self.date = date

        super().__init__(**kwargs)

    def response_is_ban(self, request, response):
        return response.status == 403 or response.status == 500

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
        """Open lists of operations for all posibilities: type, WP."""

        workplaces = self.get_workplaces(response)

        for type in ['V', 'Z']:
            for wp in workplaces:

                wp_string = str(wp)
                yield scrapy.FormRequest.from_response(
                    response,
                    meta = {
                        'cislo_pracoviste': wp
                    },
                    formdata = {
                        OPERATION_TYPE: type,
                        DATE_INPUT : self.date,
                        WORKPLACE: wp_string,
                        SEARCH_BTN: SEARCH_BTN_TXT
                    },
                    callback = self.process_operations_list
                )

                # _-_-_-_-_-_-_-_-_-_-_-_-_-_-_-
                # break
            # break

    def get_workplaces(self, response):
        """Load list of available KPs."""

        wp_options = response.xpath(
            """//select[@name="ctl00$bodyPlaceHolder$vyberPracoviste$listPracoviste"]
            //option[not(@disabled="Disabled") and not(contains(text(),"zrušeno"))]""")

        list = []
        for wp in wp_options:
            code = wp.xpath('@value').extract_first()
            # text = kp.xpath('text()').extract_first()
            list.append(code)

        list = [101]
        return list

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

            # _-_-_-_-_-_-_-_-_-_-_-_-_-_-_-
            # break

    def parse_operation(self, response):
        """Parse and save operation item."""

        if self.is_error_message(response):
            return

        operation_item = {
            'item_type': 'RIZENI',
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
                operation_item[name] = value

        ku_string = response.xpath(
            '//p[contains(text(), "Řízení se týká nemovitostí v k.ú.")]').extract_first()
        ku_code = get_string_between_brackets(ku_string)
        operation_item['cislo_ku'] = ku_code

        # _-_-_-_-_-_-_-_-_-_-_-_-_-_-_-
        # return

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
        pprint(operation_item)
        yield operation_item

        for r in ref_requests:
            yield r


    def parse_ground_refs(self, response):
        """Parse LV references from ground - for uperation purposes."""

        ground_ref_item = {
            'item_type': 'REF_RIZENI',
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

        _, ground_ref_item['cislo_ku'] = self.parse_string_w_num(
            ground_ref_item['cislo_ku'])

        ground_ref_item['lv_item'] = {
            'cislo_lv': ground_ref_item['cislo_lv'],
            'cislo_ku': ground_ref_item['cislo_ku']
        }

        pprint(ground_ref_item)
        yield ground_ref_item


    def parse_unit_refs(self, response):
        """Parse LV references from unit - for operation purposes."""

        unit_ref_item = {
            'item_type': 'REF_RIZENI',
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

        _, unit_ref_item['cislo_ku'] = self.parse_string_w_num(
            unit_ref_item['cislo_ku'])

        unit_ref_item['lv_item'] = {
            'cislo_lv': unit_ref_item['cislo_lv'],
            'cislo_ku': unit_ref_item['cislo_ku']
        }

        pprint(unit_ref_item)
        yield unit_ref_item


    def parse_string_w_num(self, input):
        num = input[input.find('[') + 1:input.find(']')]
        string = input.replace('[' + num + ']', '').strip()
        return (string, num)


    def is_error_message(self, response):
        error_message = response.xpath(
            '//div[@id="ctl00_hlaseniOnMasterPage"]').extract_first()  # ctl00_updatePanelHlaseniOnMasterPage

        # TODO distinguish between 'not found' and 'session expired' message
        # print(error_message)
        return error_message is not None


def get_string_between_brackets(string):
    return string[string.find("(")+1:string.find(")")]

def get_connection():
    return mariadb.connect(
        user='user',
        password='password',
        database='katastr')

def is_operation_updated(operation_number, wp, state):
    query = """SELECT EXISTS(
                SELECT 1 FROM rizeni
                WHERE cislo_rizeni = %s AND
                      cislo_pracoviste = %s AND
                      stav_rizeni = %s)"""
    values = (operation_number, wp, state)
    result = None
    try:
        connection = get_connection()
        cursor = connection.cursor(buffered=True)

        cursor.execute(query, values)
        result = cursor.fetchone()
    except Exception as e:
        print(e)
    finally:
        cursor.close()
        connection.close()

    if result[0] == 1:
        return True
    return False
