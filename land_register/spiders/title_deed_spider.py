import scrapy
from urllib.parse import urljoin
from unidecode import unidecode
# from pprint import pprint
# from scrapy.crawler import CrawlerProcess
# from urllib.request import Request, urlopen
# import random


BASE_URL = 'https://nahlizenidokn.cuzk.cz/'
START_URL = 'https://nahlizenidokn.cuzk.cz/VyberLV.aspx'
KU_INPUT_ELEMENT = 'ctl00$bodyPlaceHolder$vyberObecKU$vyberKU$txtKU'
KU_SEARCH_BUTTON = 'ctl00$bodyPlaceHolder$vyberObecKU$vyberKU$btnKU'
LV_INPUT_ELEMENT = 'ctl00$bodyPlaceHolder$txtLV'
LV_SEARCH_BUTTON = 'ctl00$bodyPlaceHolder$btnVyhledat'
SEARCH_TXT = 'Vyhledat'
MAX_LV_NOT_FOUND_IN_ROW = 100


class TitleDeedSpider(scrapy.Spider):
    name = "TitleDeedSpider"
    start_urls = [START_URL]

    def __init__(self, ku_code='', **kwargs):
        self.ku_code = '600016'
        self.invalid_in_row = 0
        self.total_count = 0
        super().__init__(**kwargs)

    def response_is_ban(self, request, response):
        return response.status == 403 or response.status == 500


    def parse(self, response):
        """Enter KU code (kod katastralneho uzemia) to form."""

        cislo_lv = response.meta.get('cislo_lv', 1)
        yield scrapy.FormRequest.from_response(
            response,
            meta = {
                'cislo_lv': cislo_lv
            },
            formdata = {
                KU_INPUT_ELEMENT: self.ku_code,
                KU_SEARCH_BUTTON: SEARCH_TXT
            },
            callback = self.enter_lv_code
        )

    # def parse_again(self, response):
    #     """Parse KU code (kod katastralneho uzemia)"""
    #
    #     # print('-----------------PARSE method')
    #     print('cislo lv: {}, cislo ku: {}'.format(
    #         response.meta.get('cislo_lv', 1), self.ku_code))
    #     # print('velkost requestu: {}'.format(response.headers['Content-Length']))
    #
    #     yield scrapy.FormRequest.from_response(
    #         self.resp, # response,
    #         meta={
    #             'cislo_lv': response.meta.get('cislo_lv')
    #         },
    #         formdata={
    #             KU_INPUT_ELEMENT: self.ku_code,
    #             KU_SEARCH_BUTTON: SEARCH_TXT
    #         },
    #         callback=self.enter_lv_code,
    #         dont_filter = True
    #     )

    def enter_lv_code(self, response):
        """Enter LV code (kod listu vlastnictva) to form."""

        if self.is_error_message(response):
            #if ku code doesn't exist
            return

        self.ku_response = response
        self.process_lv(response)


    def process_lv(response, next=False):
        cislo_lv = response.meta['cislo_lv']
        if next:
            cislo_lv += 1

        yield scrapy.FormRequest.from_response(
            self.ku_response,
            meta = {
                'cislo_lv': cislo_lv
            },
            formdata = {
                LV_INPUT_ELEMENT: str(cislo_lv),
                LV_SEARCH_BUTTON: SEARCH_TXT
            },
            callback = self.parse_lv_content,
            dont_filter = True
        )

        # yield scrapy.Request(
        #     START_URL,
        #     meta = {
        #       'cislo_lv': next_lv
        #     },
        #     callback = self.parse_again,
        #     dont_filter = True
        # )


    def parse_lv_content(self, response):
        """Parse content of LV. If lv code doesn't exist,
        note it and continue."""

        # check if LV is valid
        self.total_count += 1
        if self.is_error_message(response):
            self.invalid_in_row += 1

            if self.invalid_in_row >= MAX_LV_NOT_FOUND_IN_ROW:
                pass
            else:
                self.process_lv(response, next=True)

            return

        else:
            self.invalid_in_row = 0


        # LV zakladne data
        lv_item = {
            'cislo_ku': self.ku_code,
            'cislo_lv': response.meta['cislo_lv'],
            'item_type': 'lv'
        }

        # TODO
        building_rights_table = response.xpath(
            '//table[@summary="Práva stavby"]/tbody/tr')
        if building_rights_table:
            lv_item['prava_stavby'] = 'TO DO field!'
        else:
            lv_item['prava_stavby'] = None

        yield lv_item


        # vlastnici
        # priklad: KU 733857, LV: 275
        owners_item = self.parse_owners_table(response)
        owners_item['typ_ref'] = 'LV'
        owners_item.update(lv_item)
        yield owners_item


        # pozemky
        grounds_table = response.xpath('//table[@summary="Pozemky"]/tbody/tr')
        for row in grounds_table:
            ref = row.xpath('td/a/@href').extract_first()
            url = urljoin(BASE_URL, ref)

            a = row.xpath('td/a/text()').extract_first()
            if 'součástí pozemku je stavba' in a:

                yield scrapy.Request(
                    url,
                    meta = {
                        'lv_item': lv_item
                    },
                    callback = self.parse_ground
                )


        # stavby
        buildings_table = response.xpath('//table[@summary="Stavby"]/tbody/tr')
        for row in buildings_table:
            ref = row.xpath('td/a/@href').extract_first()
            url = urljoin(BASE_URL, ref)

            yield scrapy.Request(
                url,
                meta = {
                    'lv_item': lv_item
                },
                callback = self.parse_building
            )


        # jednotky
        # priklad: KU 733857, LV 2000
        units_table = response.xpath('//table[@summary="Jednotky"]/tbody/tr')
        for row in units_table:
            ref = row.xpath('td/a/@href').extract_first()
            url = urljoin(BASE_URL, ref)

            yield scrapy.Request(
                url,
                meta = {
                    'lv_item': lv_item
                },
                callback = self.parse_unit
            )

        # next LV
        self.process_lv(response, next=True)


    def parse_ground(self, response):
        if self.is_error_message(response):
            return

        lv_item = response.meta['lv_item']

        ground_item = {
            'lv_item': lv_item,
            'item_type': 'pozemek'
        }

        # atributy
        general_table = response.xpath(
            '//table[@summary="Atributy parcely"]/tbody/tr')
        for index, row in enumerate(general_table):
            name = {
                0: 'parcelni_cislo',
                1: 'obec',
                4: 'vymera',
                5: 'typ_parcely',
                8: 'druh_pozemku'
            }.get(index)

            if name:
                value = row.xpath('td[2]/text()').extract_first()
                ground_item[name] = value

        parsed_data = self.parse_string_w_num(ground_item['obec'])
        ground_item['obec'], ground_item['cislo_obce'] = parsed_data

        ground_item['id_parcely'] = self.get_id_from_link(
            ground_item['parcelni_cislo'])

        ground_item['zpusob_ochrany_nemovitosti'] = self.parse_zom(response)
        ground_item['omezeni_vlastnickeho_prava'] = self.parse_ovp(response)
        # ground_item['jine_zapisy'] = self.parse_other_notes(response)

        yield ground_item


        # stavebni objekt
        building_object_table = response.xpath(
            '//table[@summary="Atributy stavby"]/tbody/tr')
        for index, row in enumerate(building_object_table):
            name = row.xpath('td[1]/text()').extract_first()

            if name == 'Budova bez čísla popisného nebo evidenčního:':
                # building_object_item['bez_cisel'] = True
                #TODO?
                print('---------> Budova bez čísel. KU: {}, LV: {}, parcelné číslo: {}'.format(
                    lv_item['cislo_ku'], lv_item['cislo_lv'], ground_item['parcelni_cislo'] ))
                break

            if name == 'Stavební objekt:':
                ref = row.xpath('td[2]/a/@href').extract_first()

                ground_item['cislo_stavebniho_objektu'] = row.xpath(
                    'td[2]/text()').extract_first()

                ground_item['id_stavebniho_objektu'] = self.get_id_from_link(ref)

                yield scrapy.Request(
                    url,
                    meta = {
                        'lv_item': lv_item,
                        'ground_item': ground_item
                    },
                    callback = self.parse_building_object
                )
                break


        # rizeni
        operation_refs = self.get_refs_from_detail_table(
            response, 'Řízení, v rámci kterých byl k nemovitosti zapsán cenový údaj')
        for ref in operation_refs:
            yield scrapy.Request(
                ref,
                meta = {
                    'lv_item': lv_item
                },
                callback = self.parse_operation
            )


    def parse_building_object(self, response):
        lv_item = response.meta['lv_item']
        ground_item = response.meta['ground_item']

        building_object_item = {
            'lv_item': lv_item,
            'item_type': 'stavebni_objekt',
            'id_stavebniho_objektu': ground_item.get('id_stavebniho_objektu')
        }

        # atributy
        detail_table1 = response.xpath(
            '//table[@class="detail detail2columns"]/tbody/tr')
        for index, row in enumerate(detail_table1):
            name = {
                0: 'cisla_popis_evid',
                1: 'typ',
                2: 'zpusob_vyuziti'
            }.get(index)

            if name is None:
                continue

            value = row.xpath('td[2]/text()').extract_first()
            building_object_item[name] = value

        detail_table2 = response.xpath('//table[@class="detail"]/tbody/tr')
        for index, row in enumerate(detail_table2):
            name = {
                0: 'datum_dokonceni',
                1: 'pocet_bytu',
                2: 'zastavena_plocha',
                4: 'podlahova_plocha',
                5: 'pocet_podlazi'
            }.get(index)

            if name is None:
                continue

            value = row.xpath('td[2]/text()').extract_first()
            building_object_item[name] = value

        yield building_object_item


    def parse_building(self, response):
        if self.is_error_message(response):
            return

        lv_item = response.meta['lv_item']

        building_item = {
            'lv_item': lv_item,
            'item_type': 'stavba'
        }

        # atributy
        building_table = response.xpath(
            '//table[@class="Atributy stavby"]/tbody/tr')
        for index, row in enumerate(building_table):
            name = row.xpath('td[1]/text()').extract_first()
            name = {
                'Obec:': 'obec',
                'Část obce:': 'cast_obce',
                'Typ stavby:': 'typ_stavby',
                'Způsob využití:': 'zpusob_vyuziti'
            }.get(name)

            if name:
                value = row.xpath('td[2]/text()').extract_first()
                building_item[name] = value

        # obec, cislo obce
        parsed_data = self.parse_string_w_num(building_item['obec'])
        building_item['obec'], building_item['cislo_obce'] = parsed_data

        # cast obce, cislo casti obce
        parsed_data = self.parse_string_w_num(building_item['cast_obce'])
        building_item['cast_obce'], building_item['cislo_casti_obce'] = parsed_data

        # id stavebniho objektu
        ruian_table = response.xpath(
            '//table[@summary="Informace z RÚIAN"]/tbody/tr')
        for row in ruian_table:
            name = row.xpath('td[1]/text()').extract_first()

            if name == 'Stavební objekt:':
                ref = row.xpath('td[2]/a/@href').extract_first()
                building_item['id_stavebniho_objektu'] = self.get_id_from_link(ref)
                break

        yield building_item


    def parse_unit(self, response):
        if self.is_error_message(response):
            return

        lv_item = response.meta['lv_item']

        unit_item = {
            'lv_item': lv_item,
            'item_type': 'jednotka'
        }

        # atributy
        unit_table = response.xpath(
            '//table[@class="Atributy jednotky"]/tbody/tr')
        for index, row in enumerate(unit_table):
            name = row.xpath('td[1]/text()').extract_first()
            name = {
                'Číslo jednotky:': 'cislo_jednotky',
                'Typ jednotky:': 'typ_jednotky',
                'Způsob využití:': 'zpusob_vyuziti',
                'Podíl na společných částech:': 'podil_na_spol_castech'
            }.get(name)

            if name:
                value = row.xpath('td[2]/text()').extract_first()
                unit_item[name] = value

        # data v tabulkach
        unit_item['zpusob_ochrany_nemovitosti'] = self.parse_zom(response)
        unit_item['omezeni_vlastnickeho_prava'] = self.parse_ovp(response)
        unit_item['jine_zapisy'] = self.parse_other_notes(response)

        yield unit_item


    def parse_operation(self, response):
        if self.is_error_message(response):
            return

        lv_item = response.meta['lv_item']
        typ_ref = ?????

        operation_item = {
            'lv_item': lv_item,
            'item_type': 'rizeni',
            'typ_ref': typ_ref
        }

        # atributy
        operation_table = response.xpath(
            '//table[@class="Atributy jednotky"]/tbody/tr')
        for row in operation_table:
            name = row.xpath('td[1]/text()').extract_first()
            name = {
                'Číslo řízení:': 'cislo_rizeni',
                'Datum přijetí:': 'datum_prijeti',
                'Stav řízení:': 'stav_rizeni'
            }.get(name)

            if name:
                value = row.xpath('td[2]/text()').extract_first()
                operation_item[name] = value

        yield operation_item


        # ucastnici rizeni
        participants_item = {
            'lv_item': lv_item,
            'item_type': 'ucastnici_rizeni',
            'cislo_rizeni': operation_item['cislo_rizeni'],
            'ucastnici': []
        }

        participants_table = response.xpath(
            '//table[@class="Účastníci řízení"]/tbody/tr')
        for index, row in enumerate(participants_table):
            participant = {
                'poradove_cislo': index
                'jmeno': row.xpath('td[1]/text()').extract_first(),
                'typ': row.xpath('td[2]/text()').extract_first()
            }
            participants_item['ucastnici'].append(participant)

        yield participants_item


        # provedene operace
        performed_ops_item = {
            'lv_item': lv_item,
            'item_type': 'provedene_operace_rizeni',
            'cislo_rizeni': operation_item['cislo_rizeni'],
            'provedene_operace': []
        }

        performed_ops_table = response.xpath(
            '//table[@class="Provedené operace"]/tbody/tr')
        for index, row in enumerate(participants_table):
            performed_op = {
                'poradove_cislo': index
                'operace': row.xpath('td[1]/text()').extract_first(),
                'datum': row.xpath('td[2]/text()').extract_first()
            }
            performed_ops_item['provedene_operace'].append(performed_op)

        yield performed_ops


        # seznam nemovitosti
        property_list_item = {
            'lv_item': lv_item,
            'item_type': 'seznam_nemovitosti_rizeni',
            'cislo_rizeni': operation_item['cislo_rizeni'],
            'seznam_nemovitosti': []
        }

        property_list_table = response.xpath(
            '//table[@class="Seznam nemovitostí, ke kterým byl v rámci řízení zapsán cenový údaj"]/tbody/tr')
        type = 'NEZNAMY'
        index = 1
        for index in property_list_table:
            type_name = row.xpath('th[1]/text()').extract_first()
            if type_name:
                type = {
                    'Parcely': 'PARCELA',
                    'Jednotky': 'JEDNOTKA'
                }.get(type_name)
                continue

            property = {
                'poradove_cislo': index,
                'typ': type,
                'cislo': row.xpath('td[1]/text()').extract_first()
            }
            property_list_item['seznam_nemovitosti'].append(property)
            index += 1

        yield property_list_item


    def parse_owners_table(self, response):
        owners_table = response.xpath(
            '//table[@summary="Vlastníci, jiní oprávnění"]/tbody/tr')

        owners_item['vlastnici'] = []
        for row in owners_table:
            # header check
            if row.xpath('th/text()').extract_first() is not None:
                continue

            owner = {}
            owner['vlastnik'] = row.xpath('td[1]/text()').extract_first()
            owner['podil'] = row.xpath('td[2]/text()').extract_first()

            owners_item['vlastnici'].append(owner)

        return owners_item


    def parse_zom(self, response):
        zom_table = response.xpath(
            '//table[@summary="Způsob ochrany nemovitosti"]/tbody/tr')
        zom = ''
        for row in zom_table:
            if zom:
                zom += ';'
            zom += row.xpath('td[1]/text()').extract_first()

    def parse_ovp(self, response):
        ovp_table = response.xpath(
            '//table[@summary="Omezení vlastnického práva"]/tbody/tr')
        ovp = ''
        for row in ovp_table:
            if ovp:
                ovp += ';'
            ovp += row.xpath('td[1]/text()').extract_first()

    def parse_other_notes(self, response):
        on_table = response.xpath(
            '//table[@summary="Jiné zápisy"]/tbody/tr')
        on = ''
        for row in on_table:
            if on:
                on += ';'
            on += row.xpath('td[1]/text()').extract_first()


    def parse_string_w_num(self, input):
        num = input[input.find('[') + 1:input.find(']')]
        string = input.replace('[' + num + ']', '').strip()
        return (string, num)

    def get_id_from_link(self, link):
        return link.rsplit('/', 1)[-1]

    def get_refs_from_detail_table(self, response, table_name):
        table = response.xpath(
            '//table[@summary="{}"]/tbody/tr'.format(table_name))
        for row in table:
            refs = row.xpath('td/a/@href').extract()
            for ref in refs:
                url = urljoin(BASE_URL, ref)
                yield url

    def is_error_message(self, response):
        error_message = response.xpath(
            '//div[@id="ctl00_hlaseniOnMasterPage"]').extract_first()  # ctl00_updatePanelHlaseniOnMasterPage

        # TODO distinguish between 'not found' and 'session expired' message
        # print(error_message)
        return error_message is not None


def get_simple_string(str):
    """ Remove diacritics and punctation and replace spaces with underscores"""
    return unidecode(str).translate(None, string.punctuation)
