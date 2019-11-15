import scrapy
from urllib.parse import urljoin
from datetime import datetime
from scrapy.utils.project import get_project_settings
from land_register import db_handler
from pprint import pprint

BASE_URL = 'https://nahlizenidokn.cuzk.cz/'
START_URL = 'https://nahlizenidokn.cuzk.cz/VyberLV.aspx'
KU_INPUT_ELEMENT = 'ctl00$bodyPlaceHolder$vyberObecKU$vyberKU$txtKU'
KU_SEARCH_BUTTON = 'ctl00$bodyPlaceHolder$vyberObecKU$vyberKU$btnKU'
LV_INPUT_ELEMENT = 'ctl00$bodyPlaceHolder$txtLV'
LV_SEARCH_BUTTON = 'ctl00$bodyPlaceHolder$btnVyhledat'
SEARCH_TXT = 'Vyhledat'


class LandRegisterSpider(scrapy.Spider):
    """Scraping of land register."""

    name = "LandRegisterSpider"
    start_urls = [START_URL]
    custom_settings = {
        'ITEM_PIPELINES': {
            'land_register.pipelines.land_register_pipeline.LandRegisterPipeline': 100
        }
    }

    def __init__(self, ku_code, job_id=None, starting_lv=1, **kwargs):
        self.ku_code = ku_code
        self.job_id = job_id
        self.starting_lv = int(starting_lv)

        self.invalid_in_row = 0
        self.total_count = 0
        self.success = False

        settings = get_project_settings()
        self.max_invalid_items_in_row = settings['MAX_INVALID_ITEMS_IN_ROW']

        self.load_lv_list()

        super().__init__(**kwargs)

    def start_requests(self):
        yield scrapy.Request(
            url=START_URL,
            callback=self.enter_ku_code
            # errback=self.errback
        )

    def errback(self, failure):
        from scrapy.spidermiddlewares.httperror import HttpError
        from twisted.internet.error import DNSLookupError
        from twisted.internet.error import TimeoutError, TCPTimedOutError

        # log all failures
        self.logger.error(repr(failure))

        # in case you want to do something special for some errors,
        # you may need the failure's type:

        if failure.check(HttpError):
            # these exceptions come from HttpError spider middleware
            # you can get the non-200 response
            response = failure.value.response
            self.logger.error('HttpError on %s', response.url)

        elif failure.check(DNSLookupError):
            # this is the original request
            request = failure.request
            self.logger.error('DNSLookupError on %s', request.url)

        elif failure.check(TimeoutError, TCPTimedOutError):
            request = failure.request
            self.logger.error('TimeoutError on %s', request.url)

    def response_is_ban(self, request, response):
        """Defines, which status codes mean ban."""

        return response.status == 403 or response.status == 500

    def load_lv_list(self):
        """Load list of existing LVs -> have been scraped before."""

        db = db_handler.get_dataset()
        lv_list = db.query("""
            SELECT cislo_lv, MAX(datum), existuje
                FROM log_lv
                WHERE cislo_ku = {}
                  AND cislo_lv >= {}
                GROUP BY cislo_lv
                ORDER BY cislo_lv ASC""".format(
            self.ku_code, self.starting_lv)
        )

        self.lv_list = sorted(lv['cislo_lv'] for lv in lv_list
                              if lv['existuje'] is True)

    def get_start_lv_code(self):
        """Get first LV number to scrape. From LV list or from starting_lv."""

        for lv_code in self.lv_list:
            if lv_code >= self.starting_lv:
                return lv_code

        return self.starting_lv

    def get_next_lv_code(self, last_lv_code):
        """Get next LV number to scrape. From LV list or simple next number."""

        for lv_code in self.lv_list:
            if lv_code > last_lv_code:
                return lv_code

        return last_lv_code + 1

    def check_for_invalid_items_in_row(self, lv_code):
        """Check if it's allowed to mark invalid items in a row.
        The condition is that if exist list of existing lv numbers, currently
        scraping lv num has to be bigger than highest lv number in the list."""

        if not self.lv_list:
            return True

        if lv_code > self.lv_list[-1]:
            return True

        return False

    def closed(self, reason):
        """Called when spider is closed due to any error or success."""

        # F (Finished) if success, E (Error) if error
        status = 'F' if self.success else 'E'
        self.save_job_log(status)

    def save_lv_log(self, lv_code, exists):
        """Save LV log, for current job id."""

        db = db_handler.get_dataset()
        db['log_lv'].insert(dict(
            id_ulohy=self.job_id,
            cislo_lv=lv_code,
            cislo_ku=self.ku_code,
            existuje=exists
        ))

        if not exists:
            self.set_lv_erasure(lv_code)

    def save_job_log(self, status):
        """Save overall status for KU job."""

        db = db_handler.get_dataset()
        db['log_uloha'].update(dict(
            id=self.job_id,
            stav=status,
            datum_konce=datetime.now()
        ), ['id'])

    def set_lv_erasure(self, lv_code):
        """Check if not existing LV existed before. If yes, mark erasure."""

        db = db_handler.get_dataset()
        query = dict(cislo_ku=self.ku_code, cislo_lv=lv_code)

        result = db['lv'].find(**query, order_by='datum_zmeny', _limit=1)
        for r in result:
            r['cislo_zaznamu'] += 1
            r['bylo_vymazano'] = True
            r['prava_stavby'] = None
            db['lv'].insert(r)
            break

    def enter_ku_code(self, response):
        """Enter KU code (kod katastralneho uzemia) to form."""

        # use lv_code from response if is passed in meta (not used right now)
        lv_code = (response.meta['cislo_lv']
                   if response.meta.get('cislo_lv')
                   else self.get_start_lv_code())

        yield scrapy.FormRequest.from_response(
            response,
            meta={
                'cislo_lv': lv_code
            },
            formdata={
                KU_INPUT_ELEMENT: self.ku_code,
                KU_SEARCH_BUTTON: SEARCH_TXT
            },
            callback=self.enter_lv_code
        )

    def enter_lv_code(self, response):
        """Enter LV code (kod listu vlastnictva) to form."""

        if self.is_error_message(response):
            # if ku code doesn't exist
            return

        self.ku_response = response
        lv_code = response.meta['cislo_lv']

        yield scrapy.FormRequest.from_response(
            self.ku_response,
            meta={
                'cislo_lv': lv_code
            },
            formdata={
                LV_INPUT_ELEMENT: str(lv_code),
                LV_SEARCH_BUTTON: SEARCH_TXT
            },
            callback=self.parse_lv_content,
            dont_filter=True
        )

    def parse_lv_content(self, response):
        """Parse content of LV. If lv code doesn't exist,
        note it and continue."""

        lv_code = response.meta['cislo_lv']

        is_error = self.is_error_message(response)

        # if no error occurs, check if form expired,
        # if yes end processing
        if not is_error and not is_valid_lv_form(response):
            self.success = False
            return

        # if the same content
        # if not is_error and no_lv_content(response):
        #     yield from self.enter_ku_code(response)

        # check if LV is valid or if has been reached maximum number
        # of not existed LVs
        self.total_count += 1
        if is_error:

            self.save_lv_log(lv_code, exists=False)

            if self.check_for_invalid_items_in_row(lv_code):
                self.invalid_in_row += 1

            if self.invalid_in_row < self.max_invalid_items_in_row:

                lv_code += 1
                yield scrapy.FormRequest.from_response(
                    self.ku_response,
                    meta={
                        'cislo_lv': lv_code
                    },
                    formdata={
                        LV_INPUT_ELEMENT: str(lv_code),
                        LV_SEARCH_BUTTON: SEARCH_TXT
                    },
                    callback=self.parse_lv_content,
                    dont_filter=True
                )

            else:
                self.success = True

            return

        else:
            self.save_lv_log(lv_code, exists=True)
            self.invalid_in_row = 0

        # LV
        lv_item = {
            'item_type': 'LV',
            'cislo_lv': lv_code,
            'data': {
                'cislo_ku': self.ku_code,
                'cislo_lv': lv_code,
                'prava_stavby': self.parse_building_rights(response),
                'vlastnici': self.parse_owners(response)
            }
        }

        yield lv_item

        # pozemky
        grounds_table = response.xpath('//table[@summary="Pozemky"]/tbody/tr')
        for row in grounds_table:
            ref = row.xpath('td/a/@href').extract_first()
            url = urljoin(BASE_URL, ref)

            yield scrapy.Request(
                url,
                meta={
                    'cislo_lv': lv_code
                },
                callback=self.parse_ground
            )

        # stavby
        buildings_table = response.xpath('//table[@summary="Stavby"]/tbody/tr')
        for row in buildings_table:
            ref = row.xpath('td/a/@href').extract_first()
            url = urljoin(BASE_URL, ref)

            yield scrapy.Request(
                url,
                meta={
                    'cislo_lv': lv_code
                },
                callback=self.parse_building
            )

        # jednotky
        units_table = response.xpath('//table[@summary="Jednotky"]/tbody/tr')
        for row in units_table:
            ref = row.xpath('td/a/@href').extract_first()
            url = urljoin(BASE_URL, ref)

            yield scrapy.Request(
                url,
                meta={
                    'cislo_lv': lv_code
                },
                callback=self.parse_unit
            )

        # next LV
        next_lv_code = self.get_next_lv_code(int(lv_code))
        yield scrapy.FormRequest.from_response(
            self.ku_response,
            meta={
                'cislo_lv': next_lv_code
            },
            formdata={
                LV_INPUT_ELEMENT: str(next_lv_code),
                LV_SEARCH_BUTTON: SEARCH_TXT
            },
            callback=self.parse_lv_content,
            dont_filter=True
        )

    def parse_ground(self, response):
        """Ground (pozemek) parsing. In this method is also called
        building object parsing (stavebni objekt)."""

        if self.is_error_message(response):
            return

        ground_item = {
            'cislo_lv': response.meta['cislo_lv'],
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

            if not name:
                continue

            value = row.xpath('td[2]/text()').extract_first()
            if index in [0, 1, 2, 3, 9]:
                value = row.xpath('td[2]/a/text()').extract_first()

            if index == 0:
                ref = row.xpath('td[2]/a/@href').extract_first()
                ground_data['ext_id_parcely'] = get_id_from_link(ref)

            ground_data[name] = value

        if ground_data.get('obec'):
            parsed_data = parse_string_w_num(ground_data['obec'])
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
                    # 'lv_item': lv_item,
                    'cislo_lv': response.meta['cislo_lv'],
                    'item_type': 'STAVEBNI_OBJEKT',
                    'data': {
                        'ext_id_parcely': ground_data.get('ext_id_parcely'),
                        'cisla_popis_evid': 'BEZ_CISEL'
                    }
                }

                yield building_object_item
                break

            if name == 'Stavební objekt:':
                url = row.xpath('td[2]/a/@href').extract_first()

                ground_data['cislo_stavebniho_objektu'] = row.xpath('td[2]/a/text()').extract_first()
                ground_data['ext_id_stavebniho_objektu'] = get_id_from_link(url)

                building_object_ref = {
                    'cislo_lv': response.meta['cislo_lv'],
                    'item_type': 'STAVEBNI_OBJEKT_REF',
                    'data': {
                        'url': url,
                        'ext_id_parcely': ground_data.get('ext_id_parcely'),
                        'ext_id_stavebniho_objektu': ground_data.get('ext_id_stavebniho_objektu')
                    }
                }

                yield building_object_ref

                # MOVED TO SEPARATED PARSER
                #
                # yield scrapy.Request(
                #     url,
                #     meta={
                #         'cislo_lv': response.meta['cislo_lv'],
                #         'ground_item': ground_data
                #     },
                #     callback=self.parse_building_object
                # )

                break

        ground_item['data'] = ground_data
        yield ground_item

    def parse_building_object(self, response):
        """Building object (staveni objekt) parsing."""

        ground_item = response.meta['ground_item']

        building_object_item = {
            'cislo_lv': response.meta['cislo_lv'],
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
        yield building_object_item

    def parse_building(self, response):
        """Building (stavba) parsing."""

        if self.is_error_message(response):
            return

        building_item = {
            'cislo_lv': response.meta['cislo_lv'],
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
            parsed_data = parse_string_w_num(building_data['obec'])
            building_data['obec'], building_data['cislo_obce'] = parsed_data

        # cast obce, cislo casti obce
        if building_data.get('cast_obce'):
            parsed_data = parse_string_w_num(building_data['cast_obce'])
            building_data['cast_obce'], building_data['cislo_casti_obce'] = parsed_data

        # id stavebniho objektu
        ruian_table = response.xpath(
            '//table[@summary="Informace z RÚIAN"]/tr')
        for row in ruian_table:
            name = row.xpath('td[1]/text()').extract_first()

            if name == 'Stavební objekt:':
                ref = row.xpath('td[2]/a/@href').extract_first()
                building_data['ext_id_stavebniho_objektu'] = get_id_from_link(ref)
                break

        building_data['vlastnici'] = self.parse_owners(response)

        # parse operations only from separated spider
        # building_data['rizeni'] = self.parse_operations(response)

        building_item['data'] = building_data
        yield building_item

    def parse_unit(self, response):
        """Unit (jednotka) parsing."""

        if self.is_error_message(response):
            return

        unit_item = {
            'cislo_lv': response.meta['cislo_lv'],
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
        for row in owners_table:
            # header check
            if row.xpath('th/text()').extract_first() is not None:
                continue

            owner = {}
            owner_string = row.xpath('td[1]/text()').extract_first()
            if owner_string:
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

    def is_error_message(self, response):
        """Checks if error message appeared on a page."""

        error_message = response.xpath(
            '//div[@id="ctl00_hlaseniOnMasterPage"]/span/text()').extract_first()

        if error_message and error_message != 'Zadaný LV nebyl nalezen!':
            self.logger.info(error_message)
        return error_message is not None


def no_lv_content(response):
    """Check if url of response is not first page."""

    return response.url == START_URL


def is_valid_lv_form(response):
    """Check if LV form is valid."""

    header = response.xpath('//h1/text()').extract_first()
    if header == 'Vyhledání LV':
        return False
    elif header == 'Seznam nemovitostí na LV':
        return True
    return True


def parse_string_w_num(input):
    """Parses string in format '$some_string$ [$some_number$]' into
    separated string and number."""

    num = input[input.find('[') + 1:input.find(']')]
    string = input.replace('[' + num + ']', '').strip()
    return (string, num)


def get_id_from_link(link):
    """Simply returns last substring after '/' from link."""

    return link.rsplit('/', 1)[-1]
