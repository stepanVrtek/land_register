from urllib.request import Request, urlopen
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import random
import ssl

def generate():
    # Ignore SSL
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE

    ua = UserAgent() # From here we generate a random user agent
    proxies = [] # Will contain proxies [ip, port]

    proxies_req = Request('https://www.sslproxies.org/')
    proxies_req.add_header('User-Agent', ua.random)
    proxies_doc = urlopen(proxies_req, context=ctx).read().decode('utf8')

    soup = BeautifulSoup(proxies_doc, 'html.parser')
    proxies_table = soup.find(id='proxylisttable')

    # Save proxies in the array
    for row in proxies_table.tbody.find_all('tr'):
        proxies.append({
          'ip':   row.find_all('td')[0].string,
          'port': row.find_all('td')[1].string
        })

    text_file = open("proxies_list.txt", "w")

    for proxy in proxies:
        text_file.write('https://' + proxy['ip'] + ':' + proxy['port'] + '\n')

    text_file.close()


if __name__ == '__main__':
    generate()
