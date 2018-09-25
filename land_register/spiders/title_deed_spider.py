import scrapy
from pprint import pprint
from urllib.parse import urljoin
from unidecode import unidecode

BASE_URL = 'https://nahlizenidokn.cuzk.cz/'
START_URL = 'https://nahlizenidokn.cuzk.cz/VyberLV.aspx'
KU_INPUT_ELEMENT = 'ctl00$bodyPlaceHolder$vyberObecKU$vyberKU$txtKU'
KU_SEARCH_BUTTON = 'ctl00$bodyPlaceHolder$vyberObecKU$vyberKU$btnKU'
LV_INPUT_ELEMENT = 'ctl00$bodyPlaceHolder$txtLV'
LV_SEARCH_BUTTON = 'ctl00$bodyPlaceHolder$btnVyhledat'
SEARCH_TXT = 'Vyhledat'
MAX_LV_NOT_FOUND_IN_ROW = 500


class TitleDeedSpider(scrapy.Spider):
    name = "TitleDeedSpider"
    start_urls = [START_URL]

    def __init__(self, ku_code, **kwargs):
        self.ku_code = ku_code
        self.invalid_in_row = 0
        self.total_count = 0
        super().__init__(**kwargs)

    def response_is_ban(self, request, response):
        return response.status == 403 or response.status == 500


    def parse(self, response):
        """Enter KU code (kod katastralneho uzemia) to form."""

        cislo_lv = response.meta.get('cislo_lv', 55)

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
        cislo_lv = response.meta['cislo_lv']

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

    def parse_lv_content(self, response):
        """Parse content of LV. If lv code doesn't exist,
        note it and continue."""

        # check if LV is valid or if has been reached maximum number
        # of not existed LVs
        self.total_count += 1
        if self.is_error_message(response):
            self.invalid_in_row += 1

            if self.invalid_in_row < MAX_LV_NOT_FOUND_IN_ROW:

                cislo_lv = response.meta['cislo_lv'] + 1
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

            return
        else:
            self.invalid_in_row = 0


        # LV
        lv_item = {
            'cislo_ku': self.ku_code,
            'cislo_lv': response.meta['cislo_lv'],
            'item_type': 'LV'
        }

        lv_item['prava_stavby'] = self.parse_building_rights(response)
        lv_item['vlastnici'] = self.parse_owners(response)

        pprint(lv_item)
        yield lv_item


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

        #################################################
        return


        # next LV
        cislo_lv = response.meta['cislo_lv'] + 1
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


    def parse_ground(self, response):
        """Ground parsing."""

        if self.is_error_message(response):
            return

        lv_item = response.meta['lv_item']

        ground_item = {
            'lv_item': lv_item,
            'item_type': 'POZEMEK'
        }

        # atributy
        ground_table = response.xpath(
            '//table[@summary="Atributy parcely"]/tr')
        for index, row in enumerate(ground_table):
            name = row.xpath('td[1]/text()').extract_first()

            name = {
                0: 'parcelni_cislo',
                1: 'obec',
                4: 'vymera',
                5: 'typ_parcely',
                8: 'druh_pozemku'
            }.get(index)

            # name = {
            #     'Parcelní číslo:': 'parcelni_cislo',
            #     'Obec:': 'obec',
            #     'Výměra [m2]:': 'vymera',
            #     'Typ parcely:': 'typ_parcely',
            #     'Druh pozemku:': 'druh_pozemku'
            # }.get(name)

            if not name:
                continue

            value = row.xpath('td[2]/text()').extract_first()
            if index in [0, 1, 2, 3, 9]:
                value = row.xpath('td[2]/a/text()').extract_first()

            if index == 0:
                ref = row.xpath('td[2]/a/@href').extract_first()
                ground_item['ext_id_parcely'] = self.get_id_from_link(ref)

            ground_item[name] = value

        if ground_item.get('obec'):
            parsed_data = self.parse_string_w_num(ground_item['obec'])
            ground_item['obec'], ground_item['cislo_obce'] = parsed_data

        ground_item['zpusob_ochrany_nemovitosti'] = self.parse_zom(response)
        ground_item['omezeni_vlastnickeho_prava'] = self.parse_ovp(response)
        # ground_item['jine_zapisy'] = self.parse_other_notes(response)
        ground_item['vlastnici'] = self.parse_owners(response)

        # parse operations only from separated spider
        # ground_item['rizeni'] = self.parse_operations(response)


        # stavebni objekt
        building_object_table = response.xpath(
            '//table[@summary="Atributy stavby"]/tr')
        for index, row in enumerate(building_object_table):
            name = row.xpath('td[1]/text()').extract_first()

            if name == 'Budova bez čísla popisného nebo evidenčního:':
                building_object_item = {
                    'lv_item': lv_item,
                    'item_type': 'STAVEBNI_OBJEKT',
                    'cisla_popis_evid': 'BEZ_CISEL'
                }
                pprint(building_object_item)
                yield building_object_item
                break

            if name == 'Stavební objekt:':
                url = row.xpath('td[2]/a/@href').extract_first()

                ground_item['cislo_stavebniho_objektu'] = row.xpath(
                    'td[2]/text()').extract_first()

                ground_item['ext_id_stavebniho_objektu'] = self.get_id_from_link(url)

                yield scrapy.Request(
                    url,
                    meta = {
                        'lv_item': lv_item,
                        'ground_item': ground_item
                    },
                    callback = self.parse_building_object
                )
                break

        pprint(ground_item)
        yield ground_item


    def parse_building_object(self, response):
        """Building object parsing."""

        lv_item = response.meta['lv_item']
        ground_item = response.meta['ground_item']

        building_object_item = {
            'lv_item': lv_item,
            'item_type': 'STAVEBNI_OBJEKT',
            'ext_id_parcely': ground_item.get('ext_id_parcely'),
            'ext_id_stavebniho_objektu': ground_item.get(
                'ext_id_stavebniho_objektu')
        }

        # atributy
        detail_table1 = response.xpath(
            '(//table[@class="detail detail2columns"])[2]/tr')
        for index, row in enumerate(detail_table1):
            name = row.xpath('td[1]/text()').extract_first()
            name = {
                'Čísla popisná nebo evidenční:': 'cisla_popis_evid',
                'Typ:': 'typ',
                'Způsob využití:': 'zpusob_vyuziti'
            }.get(name)

            if name is None:
                continue

            value = row.xpath('td[2]/text()').extract_first()
            building_object_item[name] = value

        detail_table2 = response.xpath('//table[@class="detail"]/tr')
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

        pprint(building_object_item)
        yield building_object_item


    def parse_building(self, response):
        """Building parsing."""

        if self.is_error_message(response):
            return

        building_item = {
            'lv_item': response.meta['lv_item'],
            'item_type': 'STAVBA'
        }

        # atributy
        building_table = response.xpath(
            '//table[@summary="Atributy stavby"]/tbody/tr')
        for row in building_table:
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
        if building_item.get('obec'):
            parsed_data = self.parse_string_w_num(building_item['obec'])
            building_item['obec'], building_item['cislo_obce'] = parsed_data

        # cast obce, cislo casti obce
        if building_item.get('cast_obce'):
            parsed_data = self.parse_string_w_num(building_item['cast_obce'])
            building_item['cast_obce'], building_item['cislo_casti_obce'] = parsed_data

        # id stavebniho objektu
        ruian_table = response.xpath(
            '//table[@summary="Informace z RÚIAN"]/tbody/tr')
        for row in ruian_table:
            name = row.xpath('td[1]/text()').extract_first()

            if name == 'Stavební objekt:':
                ref = row.xpath('td[2]/a/@href').extract_first()
                building_item['ext_id_stavebniho_objektu'] = self.get_id_from_link(ref)
                break

        building_item['vlastnici'] = self.parse_owners(response)

        # parse operations only from separated spider
        # building_item['rizeni'] = self.parse_operations(response)

        yield building_item


    def parse_unit(self, response):
        """Unit parsing."""

        if self.is_error_message(response):
            return

        unit_item = {
            'lv_item': response.meta['lv_item'],
            'item_type': 'JEDNOTKA'
        }

        # atributy
        unit_table = response.xpath(
            '//table[@summary="Atributy jednotky"]/tbody/tr')
        for row in unit_table:
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
        unit_item['vlastnici'] = self.parse_owners(response)

        # parse operations only from separated spider
        # unit_item['rizeni'] = self.parse_operations(response)

        yield unit_item


    def parse_owners(self, response):
        owners_table = response.xpath(
            '//table[@summary="Vlastníci, jiní oprávnění"]/tbody/tr')

        owners =[]
        for row in owners_table:
            # header check
            if row.xpath('th/text()').extract_first() is not None:
                continue

            owner = {}
            owner_string = row.xpath('td[1]/text()').extract_first()
            owner['vlastnicke_pravo'] = owner_string
            owner['jmeno'], owner['adresa'] = self.parse_owner_string(owner_string)
            owner['podil'] = row.xpath('td[2]/text()').extract_first()
            owners.append(owner)

        return owners

    def parse_owner_string(self, owner_string):
        comma_idx = owner_string.find(',')
        name = owner_string[:comma_idx].strip()
        address = owner_string[comma_idx + 1:].strip()
        return (name, address)

    def parse_operations(self, response):
        """Parse operations.
        NOT USED, OPERATIONS ARE SCRAPED ONLY FROM SEPARATED SPIDER."""
        pass
        # operations_table = response.xpath(
        #     '//table[@summary="Řízení, v rámci kterých byl k nemovitosti zapsán cenový údaj"]/tbody/tr')
        #
        # operations = []
        # for row in operations_table:
        #     # header check
        #     if row.xpath('th/text()').extract_first() is not None:
        #         continue
        #
        #     operation = {}
        #     operation['cislo_rizeni'] = row.xpath('td[1]/text()').extract_first()
        #     operations.append(operation)
        #
        # return operations


    def parse_zom(self, response):
        zom_table = response.xpath(
            '//table[@summary="Způsob ochrany nemovitosti"]/tbody/tr')
        zom = ''
        for row in zom_table:
            if zom:
                zom += ';'
            zom += row.xpath('td[1]/text()').extract_first()

        return zom

    def parse_ovp(self, response):
        ovp_table = response.xpath(
            '//table[@summary="Omezení vlastnického práva"]/tbody/tr')
        ovp = ''
        for row in ovp_table:
            if ovp:
                ovp += ';'
            ovp += row.xpath('td[1]/text()').extract_first()

        return ovp

    def parse_other_notes(self, response):
        on_table = response.xpath(
            '//table[@summary="Jiné zápisy"]/tbody/tr')
        on = ''
        for row in on_table:
            if on:
                on += ';'
            on += row.xpath('td[1]/text()').extract_first()

        return on

    def parse_building_rights(self, response):
        rights_table = response.xpath(
            '//table[@summary="Práva stavby"]/tbody/tr')
        rights = ''
        for row in rights_table:
            if rights:
                rights += ';'
            rights += row.xpath('td[1]/text()').extract_first()

        return rights

    def parse_table_as_string(self, response, table_xpath, delimiter=';'):
        table = response.xpath(table_xpath)
        string = ''
        for row in table:
            if string:
                string += delimiter
            string += row.xpath('td[1]/text()').extract_first()

        return string


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
        if error_message:
            print(error_message)
        return error_message is not None


# def get_simple_string(str):
#     """ Remove diacritics and punctation and replace spaces with underscores"""
#     return unidecode(str).translate(None, string.punctuation)
