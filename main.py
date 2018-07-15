import Spiders.SeznamNemovitosti
import requests
import time
from bs4 import BeautifulSoup

url = "http://nahlizenidokn.cuzk.cz/VyberLV.aspx"
payloadOne = {'ctl00$bodyPlaceHolder$vyberObecKU$vyberKU$txtKU': 600016}
payloadTwo = {'ctl00$bodyPlaceHolder$txtLV': 1}
spider = Spiders.SeznamNemovitosti.seznamNemovitosti()

with requests.Session() as s:
    r = s.get(url)
    session_id = s.cookies['ASP.NET_SessionId']
    print(session_id)
    print(r.headers) ##Posílá server
    #print(r.request.headers) ##Posíláme na server
    first = s.post(url, params=payloadOne)
    time.sleep(3)
    second = s.post(url, params=payloadTwo)
    print(second.headers)
    #soup = BeautifulSoup(second.text, "html.parser")
    #print(soup.text)
