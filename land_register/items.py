# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class LandRegisterItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass

class LV(scrapy.Item):
    cislo_lv = scrapy.Field()
    katastralni_uzemi = scrapy.Field()
    cislo_katastralniho_uzemi = scrapy.Field()

    vlastnici = scrapy.Field() # list of Vlastnik
    pozemky = scrapy.Field() # list of Pozemek
    stavby = scrapy.Field() # list of Stavba
    jednotky = scrapy.Field() # list of Jednotka

    prava_stavby = scrapy.Field() # list of PravoStavby # it is single attribute or table?

class PravoStavby(scrapy.Item):
    pravo = scrapy.Field()

class Vlastnik(scrapy.Item):
    vlastnik  = scrapy.Field()

class Pozemek(scrapy.Item):
    parcelni_cislo = scrapy.Field()
    obec = scrapy.Field()
    cislo_obce = scrapy.Field()
    vymera = scrapy.Field()
    typ_parcely = scrapy.Field()
    druh_pozemku = scrapy.Field()
    stavebny_objekt = scrapy.Field() #cislo

    zpusoby_ochrany_nemovitosti = scrapy.Field() # list of ZpusobOchranyNemovitosti # it is single attribute or table?
    omezeni_vlastnickeho_prava = scrapy.Field() # list of OmezeniVlastnickehoPrava # it is single attribute or table?
    jine_zapisy = scrapy.Field() # list of JinyZapis # it is single attribute or table?

    izeni = scrapy.Field() # list of Rizeni

class ZpusobOchranyNemovitosti(scrapy.Item):
    zpusob = scrapy.Field()

class OmezeniVlastnickehoPrava(scrapy.Item):
    omezeni = scrapy.Field()

class JinyZapis(scrapy.Item):
    zapis = scrapy.Field()

class StavebniObjekt(scrapy.Item):
    cisla_popisni_nebo_evidencni = scrapy.Field()
    typ = scrapy.Field()
    zpusob_vyuziti = scrapy.Field()
    datum_dokonceni = scrapy.Field()
    pocet_bytu = scrapy.Field()
    zastavena_plocha = scrapy.Field()
    podlahova_plocha = scrapy.Field()
    pocet_podlazi = scrapy.Field()

class Stavba(scrapy.Item):
    obec = scrapy.Field()
    cast_obce = scrapy.Field()
    cislo_casti_obce = scrapy.Field()
    typ_stavby = scrapy.Field()
    zpusob_vyuziti = scrapy.Field()

    vlastnici = scrapy.Field() # list of Vlastnik
    rizeni = scrapy.Field() # list of Rizeni

class Jednotka(scrapy.Item):
    cislo_jednotky = scrapy.Field()
    typ_jednotky = scrapy.Field()
    zpusob_vyuziti = scrapy.Field()
    podil_na_spolecnych_castech = scrapy.Field()

    zpusoby_ochrany_nemovitosti = scrapy.Field() # list of ZpusobOchranyNemovitosti # it is single attribute or table?
    omezeni_vlastnickeho_prava = scrapy.Field() # list of OmezeniVlastnickehoPrava # it is single attribute or table?
    jine_zapisy = scrapy.Field() # list of JinyZapis # it is single attribute or table?

    rizeni = scrapy.Field() # list of Rizeni

class Rizeni(scrapy.Item):
    cislo_rizeni = scrapy.Field()
    datum_prijeti = scrapy.Field()
    stav_rizeni = scrapy.Field()

    ucastnici_rizeni = scrapy.Field() # list of UcastnikRizeni
    provedene_operace = scrapy.Field() # list of ProvedeneOperace
    parcely = scrapy.Field() # list of ParcelaVRizeni
    jednotky = scrapy.Field() # list of JednotkaVRizeni

class UcastnikRizeni(scrapy.Item):
    jmeno = scrapy.Field()
    typ = scrapy.Field()

class ProvedenaOperace(scrapy.Item):
    operace = scrapy.Field()
    datum = scrapy.Field()

class ParcelaVRizeni(scrapy.Item):
    parcela = scrapy.Field()

class JednotkaVRizeni(scrapy.Item):
    jednotka = scrapy.Field()

