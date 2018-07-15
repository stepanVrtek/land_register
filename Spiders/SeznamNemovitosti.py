import scrapy
from urlparse import urljoin


class seznamNemovitosti(scrapy.Spider):
    name = "SeznamNemovitosti"
    start_urls = [
        "http://nahlizenidokn.cuzk.cz/ZobrazObjekt.aspx?encrypted=YM1I8cKFwRqtSN1pzaZSv-EXJsX4ZkgaroYrwUMxx0MfYRoGVqejf3vfazStQpZruJm5uurEo63eIDzBPMntEajfuuJxS92jvfJtM4YC-kCfScnV_9Xmbg=="]

    def parse(self, response):
        main_url = "http://nahlizenidokn.cuzk.cz/"
        for link in response.css(
                "[summary=Pozemky]"):  # dalsi moznost k "table.zarovnat" je ziskat primo prvni tabulku a to: "[summary=Pozemky]"
            yield {
                'link': link.css("a::attr(href)").extract()
            # kazdy link ma tvar: ZobrazObjekt.aspx?encrypted=*shitloadkodu==* ktery se musi spojit s predponou "http://nahlizenidokn.cuzk.cz/"
            }
        filename = "seznam nemovitosti.html"
        with open(filename, 'wb') as f:
            f.write(response.body)
        self.log('Saved file %s' % filename)

        next_page = response.css('table.zarovnat a::attr(href)').extract_first()
        if next_page is not None:
            next_page = urljoin(main_url, next_page)
            yield response.follow(next_page, callback=self.parse) #volam ten stejny parser jako na seznam nemovitosti, coz neni dobre, musim udelat dalsi parser
