# from urllib.request import Request, urlopen
# from bs4 import BeautifulSoup
# from fake_useragent import UserAgent
# import random
# import ssl

# ua = UserAgent()  # From here we generate a random user agent
# proxies = []  # Will contain proxies [ip, port]

# # Ignore SSL
# ctx = ssl.create_default_context()
# ctx.check_hostname = False
# ctx.verify_mode = ssl.CERT_NONE

# # Main function
# def main():

#     # Retrieve latest proxies
#     proxies_req = Request('https://www.sslproxies.org/')
#     proxies_req.add_header('User-Agent', ua.random)
#     proxies_doc = urlopen(proxies_req, context=ctx).read().decode('utf8')

#     soup = BeautifulSoup(proxies_doc, 'html.parser')
#     proxies_table = soup.find(id='proxylisttable')

#     # Save proxies in the array
#     for row in proxies_table.tbody.find_all('tr'):
#         proxies.append({
#             'ip':   row.find_all('td')[0].string,
#             'port': row.find_all('td')[1].string
#         })

#     # Choose a random proxy
#     proxy_index = random_proxy()
#     proxy = proxies[proxy_index]

#     for n in range(1, 100):
#         req = Request('https://nahlizenidokn.cuzk.cz/VyberLV.aspx')
#         req.set_proxy(proxy['ip'] + ':' + proxy['port'], 'https')

#         # Every 10 requests, generate a new proxy
#         if n % 10 == 0:
#             proxy_index = random_proxy()
#             proxy = proxies[proxy_index]

#         # Make the call
#         try:
#             urlopen(req, context=ctx).read().decode('utf8')
#             print('proxy {} is ok'.format(proxy))
#         except:  # If error, delete this proxy and find another one
#             del proxies[proxy_index]
#             print('Proxy ' + proxy['ip'] + ':' + proxy['port'] + ' deleted.')
#             proxy_index = random_proxy()
#             proxy = proxies[proxy_index]

# # Retrieve a random index proxy (we need the index to delete it if not working)


# def random_proxy():
#     return random.randint(0, len(proxies) - 1)


# if __name__ == '__main__':
#     main()


from urllib.request import Request, urlopen
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import random
import ssl

# Ignore SSL
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

def get_valid_proxies():
    return [proxy for proxy in get_proxies() if is_valid_proxy(proxy)]

def get_proxies():

    # Retrieve latest proxies
    proxies_req = Request('https://www.sslproxies.org/')
    proxies_req.add_header('User-Agent', UserAgent().random)
    proxies_doc = urlopen(proxies_req, context=ctx).read().decode('utf8')

    soup = BeautifulSoup(proxies_doc, 'html.parser')
    proxies_table = soup.find(id='proxylisttable')

    for row in proxies_table.tbody.find_all('tr'):
        ip = row.find_all('td')[0].string
        port = row.find_all('td')[1].string
        proxy = '{}:{}'.format(ip, port)
        yield proxy

def is_valid_proxy(proxy):
    req = Request('https://nahlizenidokn.cuzk.cz/VyberLV.aspx')
    req.set_proxy(proxy, 'https')
    try:
        urlopen(req, context=ctx).read().decode('utf8')
        print('Proxy {} is valid'.format(proxy))
        return True
    except Exception as e:
        print('Proxy {} is invalid - {}'.format(proxy, e))
        return False


if __name__ == '__main__':
    valid_proxies = get_valid_proxies()
