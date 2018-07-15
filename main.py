import Spiders.mainPageSpider
import requests
import time

url = "http://nahlizenidokn.cuzk.cz/VyberLV.aspx"
payloadOne = {'ctl00$bodyPlaceHolder$vyberObecKU$vyberKU$txtKU': 600016}
payloadTwo = {'ctl00$bodyPlaceHolder$txtLV': 1}
spider = Spiders.mainPageSpider

with requests.Session() as s:
    r = s.get(url)
    session_id = s.cookies['ASP.NET_SessionId']
    print(session_id)
    print(r.headers) ##Posílá server
    print(r.request.headers) ##Posíláme na server
    first = s.post(url, params = payloadOne)
    time.sleep(5)
    second = s.post(url,payloadTwo)
    print(second.text)
