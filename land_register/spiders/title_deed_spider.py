import scrapy
from urllib.parse import urljoin
from scrapy.utils.project import get_project_settings
from pprint import pprint


BASE_URL = 'https://nahlizenidokn.cuzk.cz/'
START_URL = 'https://nahlizenidokn.cuzk.cz/VyberLV.aspx'
KU_INPUT_ELEMENT = 'ctl00$bodyPlaceHolder$vyberObecKU$vyberKU$txtKU'
KU_SEARCH_BUTTON = 'ctl00$bodyPlaceHolder$vyberObecKU$vyberKU$btnKU'
LV_INPUT_ELEMENT = 'ctl00$bodyPlaceHolder$txtLV'
LV_SEARCH_BUTTON = 'ctl00$bodyPlaceHolder$btnVyhledat'
SEARCH_TXT = 'Vyhledat'


class TitleDeedSpider(scrapy.Spider):
    name = "TitleDeedSpider"
    start_urls = [START_URL]

    def __init__(self, ku_code, job_id=None, start_index=1, **kwargs):
        self.ku_code = ku_code
        self.job_id = job_id
        self.start_index = start_index

        self.invalid_in_row = 0
        self.total_count = 0
        self.success = False

        settings = get_project_settings()
        self.max_invalid_items_in_row = settings['MAX_INVALID_ITEMS_IN_ROW']

        # get_lv_list

        super().__init__(**kwargs)


    def response_is_ban(self, request, response):
        return response.status == 403 or response.status == 500


    def parse(self, response):
        """Enter KU code (kod katastralneho uzemia) to form."""

        cislo_lv = response.meta.get('cislo_lv', self.start_index)

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

        # prepare item for log
        log_item = {
            'cislo_ku': self.ku_code,
            'cislo_lv': response.meta['cislo_lv'],
            'item_type': 'LOG_LV'
        }

        # check if LV is valid or if has been reached maximum number
        # of not existed LVs
        self.total_count += 1
        if self.is_error_message(response):
            log_item['existuje'] = False
            yield log_item

            self.invalid_in_row += 1

            if self.invalid_in_row < self.max_invalid_items_in_row:

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

            else:
                # update job LOG if proccesing of KU was finished successfully
                if job_id:
                    self.success = True
                    yield {
                        'id_ulohy': self.job_id,
                        'dokonceno': True,
                        'item_type': 'LOG_ULOHY'
                    }

            return
        else:
            log_item['existuje'] = True
            yield log_item

            self.invalid_in_row = 0


        # LV
        lv_item = {
            'cislo_ku': self.ku_code,
            'cislo_lv': response.meta['cislo_lv'],
            'item_type': 'LV'
        }

        lv_item['prava_stavby'] = self.parse_building_rights(response)
        lv_item['vlastnici'] = self.parse_owners(response)

        # pprint(lv_item)
        yield lv_item


        # pozemky
        grounds_table = response.xpath('//table[@summary="Pozemky"]/tbody/tr')
        for row in grounds_table:
            ref = row.xpath('td/a/@href').extract_first()
            url = urljoin(BASE_URL, ref)

            # # only grounds with building objects
            # a = row.xpath('td/a/text()').extract_first()
            # if 'součástí pozemku je stavba' in a:

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
        """Ground (pozemek) parsing. In this method is also called
        building object parsing (stavebni objekt)."""

        if self.is_error_message(response):
            return

        lv_item = response.meta['lv_item']

        ground_item = {
            'lv_item': lv_item,
            'item_type': 'POZEMEK',
            'data': {}
        }
        ground_data = {}

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
                ground_data['ext_id_parcely'] = self.get_id_from_link(ref)

            ground_data[name] = value

        if ground_data.get('obec'):
            parsed_data = self.parse_string_w_num(ground_data['obec'])
            ground_data['obec'], ground_data['cislo_obce'] = parsed_data

        ground_data['zpusob_ochrany_nemovitosti'] = self.parse_zom(response)
        ground_data['omezeni_vlastnickeho_prava'] = self.parse_ovp(response)
        ground_data['jine_zapisy'] = self.parse_other_notes(response)
        ground_data['vlastnici'] = self.parse_owners(response)

        # parse operations only from separated spider
        # ground_data['rizeni'] = self.parse_operations(response)


        # stavebni objekt
        building_object_table = response.xpath(
            '//table[@summary="Atributy stavby"]/tr')
        for index, row in enumerate(building_object_table):
            name = row.xpath('td[1]/text()').extract_first()

            if name == 'Budova bez čísla popisného nebo evidenčního:':
                building_object_item = {
                    'lv_item': lv_item,
                    'item_type': 'STAVEBNI_OBJEKT',
                    'ext_id_parcely': ground_data.get('ext_id_parcely'),
                    'cisla_popis_evid': 'BEZ_CISEL'
                }
                # pprint(building_object_item)
                yield building_object_item
                break

            if name == 'Stavební objekt:':
                url = row.xpath('td[2]/a/@href').extract_first()

                ground_data['cislo_stavebniho_objektu'] = row.xpath(
                    'td[2]/text()').extract_first()

                ground_data['ext_id_stavebniho_objektu'] = self.get_id_from_link(url)

                yield scrapy.Request(
                    url,
                    meta = {
                        'lv_item': lv_item,
                        'ground_item': ground_data
                    },
                    callback = self.parse_building_object
                )
                break

        ground_item['data'] = ground_data
        # pprint(ground_item)
        yield ground_item


    def parse_building_object(self, response):
        """Building object (staveni objekt) parsing."""

        return

        lv_item = response.meta['lv_item']
        ground_item = response.meta['ground_item']

        building_object_item = {
            'lv_item': lv_item,
            'item_type': 'STAVEBNI_OBJEKT',
            'data': {}
        }
        building_object_data = {
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
            building_object_data[name] = value

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
            building_object_data[name] = value

        building_object_item['data'] = building_object_data
        # pprint(building_object_item)
        yield building_object_item


    def parse_building(self, response):
        """Building (stavba) parsing."""

        if self.is_error_message(response):
            return

        building_item = {
            'lv_item': response.meta['lv_item'],
            'item_type': 'STAVBA',
            'data': {}
        }
        building_data = {}

        # atributy
        building_table = response.xpath(
            '//table[@summary="Atributy stavby"]/tr')
        for row in building_table:
            name = row.xpath('td[1]/text()').extract_first()
            value = row.xpath('td[2]/text()').extract_first()
            if not value:
                value = row.xpath('td[2]/a/text()').extract_first()

            if name == 'Stavba stojí na pozemku:':
                value = row.xpath('td[2]/a/text()').extract_first()

            name = {
                'Obec:': 'obec',
                'Část obce:': 'cast_obce',
                'Typ stavby:': 'typ_stavby',
                'Způsob využití:': 'zpusob_vyuziti',
                'Stavba stojí na pozemku:': 'stoji_na_pozemku'
            }.get(name)

            if name:
                building_data[name] = value

        # obec, cislo obce
        if building_data.get('obec'):
            parsed_data = self.parse_string_w_num(building_data['obec'])
            building_data['obec'], building_data['cislo_obce'] = parsed_data

        # cast obce, cislo casti obce
        if building_data.get('cast_obce'):
            parsed_data = self.parse_string_w_num(building_data['cast_obce'])
            building_data['cast_obce'], building_data['cislo_casti_obce'] = parsed_data

        # id stavebniho objektu
        ruian_table = response.xpath(
            '//table[@summary="Informace z RÚIAN"]/tr')
        for row in ruian_table:
            name = row.xpath('td[1]/text()').extract_first()

            if name == 'Stavební objekt:':
                ref = row.xpath('td[2]/a/@href').extract_first()
                building_data['ext_id_stavebniho_objektu'] = self.get_id_from_link(ref)
                break

        building_data['vlastnici'] = self.parse_owners(response)

        # parse operations only from separated spider
        # building_data['rizeni'] = self.parse_operations(response)

        building_item['data'] = building_data
        # pprint(building_item)
        yield building_item


    def parse_unit(self, response):
        """Unit (jednotka) parsing."""

        if self.is_error_message(response):
            return

        unit_item = {
            'lv_item': response.meta['lv_item'],
            'item_type': 'JEDNOTKA'
        }
        unit_data = {}

        # atributes
        unit_table = response.xpath(
            '//table[@summary="Atributy jednotky"]/tr')
        for row in unit_table:
            name = row.xpath('td[1]/text()').extract_first()
            value = row.xpath('td[2]/text()').extract_first()
            if not name:
                name = row.xpath('td[1]/strong/text()').extract_first()
                value = row.xpath('td[2]/strong/text()').extract_first()

            name = {
                'Číslo jednotky': 'cislo_jednotky',
                'Typ jednotky:': 'typ_jednotky',
                'Způsob využití:': 'zpusob_vyuziti',
                'Podíl na': 'podil_na_spol_castech'
            }.get(name)

            if name:
                unit_data[name] = value

        # data in tables
        unit_data['zpusob_ochrany_nemovitosti'] = self.parse_zom(response)
        unit_data['omezeni_vlastnickeho_prava'] = self.parse_ovp(response)
        unit_data['jine_zapisy'] = self.parse_other_notes(response)
        unit_data['vlastnici'] = self.parse_owners(response)

        # parse operations only from separated spider
        # unit_data['rizeni'] = self.parse_operations(response)

        unit_item['data'] = unit_data
        # pprint(unit_item)
        yield unit_item


    def parse_operations(self, response):
        """Operations (řízení) parsing.
        NOT USED, OPERATIONS ARE SCRAPED ONLY FROM SEPARATED SPIDER."""
        pass


    def parse_owners(self, response):
        """Owners (vlastnici) parsing."""

        owners_table = response.xpath(
            '//table[@summary="Vlastníci, jiní oprávnění"]/tbody/tr')

        owners = []
        for idx, row in enumerate(owners_table):
            # header check
            if row.xpath('th/text()').extract_first() is not None:
                continue

            owner = {}
            owner_string = row.xpath('td[1]/text()').extract_first()
            if owner_string:
                owner['cislo_vlastnika'] = idx + 1
                owner['vlastnicke_pravo'] = owner_string
                owner['jmeno'], owner['adresa'] = self.parse_owner_string(owner_string)
                owner['podil'] = row.xpath('td[2]/text()').extract_first()
                owners.append(owner)

        return owners

    def parse_owner_string(self, owner_string):
        """Parse single owner (vlastnik) from string."""

        comma_idx = owner_string.find(',')
        name = owner_string[:comma_idx].strip()
        address = owner_string[comma_idx + 1:].strip()
        return (name, address)


    def parse_zom(self, response):
        """Parsing table of 'Způsob ochrany nemovitosti'."""

        zom_table = response.xpath(
            '//table[@summary="Způsob ochrany nemovitosti"]/tbody/tr')
        zom = ''
        for row in zom_table:
            if zom:
                zom += ';'
            zom += row.xpath('td[1]/text()').extract_first()

        return zom if zom else None


    def parse_ovp(self, response):
        """Parsing table of 'Omezení vlastnického práva'."""

        ovp_table = response.xpath(
            '//table[@summary="Omezení vlastnického práva"]/tbody/tr')
        ovp = ''
        for row in ovp_table:
            if ovp:
                ovp += ';'
            ovp += row.xpath('td[1]/text()').extract_first()

        return ovp if ovp else None


    def parse_other_notes(self, response):
        """Parsing table of 'Jiné zápisy'."""

        on_table = response.xpath(
            '//table[@summary="Jiné zápisy"]/tbody/tr')
        on = ''
        for row in on_table:
            if on:
                on += ';'
            on += row.xpath('td[1]/text()').extract_first()

        return on if on else None


    def parse_building_rights(self, response):
        """Parsing table of 'Práva stavby'."""

        rights_table = response.xpath(
            '//table[@summary="Práva stavby"]/tbody/tr')
        rights = ''
        for row in rights_table:
            if rights:
                rights += ';'
            rights += row.xpath('td[1]/text()').extract_first()

        return rights if rights else None



    def parse_string_w_num(self, input):
        """Parses string in format '$some_string$ [$some_number$]' into
        separated string and number."""

        num = input[input.find('[') + 1:input.find(']')]
        string = input.replace('[' + num + ']', '').strip()
        return (string, num)


    def get_id_from_link(self, link):
        """Simply returns last substring after '/' from link."""

        return link.rsplit('/', 1)[-1]



    def is_error_message(self, response):
        error_message = response.xpath(
            '//div[@id="ctl00_hlaseniOnMasterPage"]/span/text()').extract_first()  # ctl00_updatePanelHlaseniOnMasterPage

        # TODO distinguish between 'not found' and 'session expired' message
        if error_message and error_message != 'Zadaný LV nebyl nalezen!':
            print(error_message)
        return error_message is not None
