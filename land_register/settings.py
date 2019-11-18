# -*- coding: utf-8 -*-

# Scrapy settings for land_register project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://doc.scrapy.org/en/latest/topics/settings.html
#     https://doc.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://doc.scrapy.org/en/latest/topics/spider-middleware.html

BOT_NAME = 'land_register'

SPIDER_MODULES = ['land_register.spiders']
NEWSPIDER_MODULE = 'land_register.spiders'

# Crawl responsibly by identifying yourself (and your website) on the user-agent
# USER_AGENT = 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/22.0.1207.1 Safari/537.1'

# Obey robots.txt rules
ROBOTSTXT_OBEY = False

# Configure maximum concurrent requests performed by Scrapy (default: 16)
CONCURRENT_REQUESTS = 1  # 50

# Configure a delay for requests for the same website (default: 0)
# See https://doc.scrapy.org/en/latest/topics/settings.html#download-delay
# See also autothrottle settings and docs
# DOWNLOAD_DELAY = 3
# The download delay setting will honor only one of:
CONCURRENT_REQUESTS_PER_DOMAIN = 1  # 50
# CONCURRENT_REQUESTS_PER_IP = 16

# Disable cookies (enabled by default)
# COOKIES_ENABLED = False

# Disable Telnet Console (enabled by default)
# TELNETCONSOLE_ENABLED = False

# Override the default request headers:
# DEFAULT_REQUEST_HEADERS = {
#   'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
#   'Accept-Language': 'en',
# }

# Enable or disable spider middlewares
# See https://doc.scrapy.org/en/latest/topics/spider-middleware.html
# SPIDER_MIDDLEWARES = {
#    'land_register.middlewares.LandRegisterSpiderMiddleware': 543,
# }

# Enable or disable downloader middlewares
# See https://doc.scrapy.org/en/latest/topics/downloader-middleware.html
DOWNLOADER_MIDDLEWARES = {
    # 'rotating_proxies.middlewares.RotatingProxyMiddleware': 610,
    # 'rotating_proxies.middlewares.BanDetectionMiddleware': 620,

    'land_register.middlewares.RandomProxyMiddleware': 350,
    'scrapy_user_agents.middlewares.RandomUserAgentMiddleware': 390,
    'scrapy.downloadermiddlewares.httpproxy.HttpProxyMiddleware': 400,
    'land_register.middlewares.LandRegisterDownloaderMiddleware': 543,
    'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
    # 'scrapy_fake_useragent.middleware.RandomUserAgentMiddleware': 800
}

# Enable or disable extensions
# See https://doc.scrapy.org/en/latest/topics/extensions.html
# EXTENSIONS = {
#    'scrapy.extensions.telnet.TelnetConsole': None,
# }

# Configure item pipelines
# See https://doc.scrapy.org/en/latest/topics/item-pipeline.html

# Pipelins are defined in spiders as custom settings
# ITEM_PIPELINES = {
#     'land_register.pipelines.land_register_pipeline.LandRegisterPipeline': 100,
#     'land_register.pipelines.building_objects_pipeline.BuildingObjectsPipeline': 101,
#     'land_register.pipelines.operations_pipeline.OperationsPipeline': 200
# }

# Enable and configure the AutoThrottle extension (disabled by default)
# See https://doc.scrapy.org/en/latest/topics/autothrottle.html
# AUTOTHROTTLE_ENABLED = False
# # The initial download delay
# AUTOTHROTTLE_START_DELAY = 1  # 5
# # The maximum download delay to be set in case of high latencies
# AUTOTHROTTLE_MAX_DELAY = 3  # 60
# # The average number of requests Scrapy should be sending in parallel to
# # each remote server
# # AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
# # Enable showing throttling stats for every response received:
# AUTOTHROTTLE_DEBUG = True

# Enable and configure HTTP caching (disabled by default)
# See https://doc.scrapy.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings
# HTTPCACHE_ENABLED = True
# HTTPCACHE_EXPIRATION_SECS = 0
# HTTPCACHE_DIR = 'httpcache'
# HTTPCACHE_IGNORE_HTTP_CODES = []
# HTTPCACHE_STORAGE = 'scrapy.extensions.httpcache.FilesystemCacheStorage'


# Proxy endpoints
ROTATING_PROXY_LIST = [
    '95.211.175.167:13151',
    '95.211.175.225:13151'
]
# ROTATING_PROXY_LIST_PATH = 'land_register/proxies_list.txt'


# # Export
# FEED_FORMAT = 'csv'


# Scrape data in order in which data are fetched
DEPTH_PRIORITY = 1
SCHEDULER_DISK_QUEUE = 'scrapy.squeues.PickleFifoDiskQueue'
SCHEDULER_MEMORY_QUEUE = 'scrapy.squeues.FifoMemoryQueue'


TELNETCONSOLE_PORT = None

LOG_LEVEL = 'INFO'


RETRY_ENABLED = True
RETRY_HTTP_CODES = [500, 502, 503, 504, 403, 404, 408]
RETRY_TIMES = 500
DOWNLOAD_TIMEOUT = 180
# HTTPERROR_ALLOWED_CODES = [500, 403]


# Settings for land_register project

# DB connection
DB_CONNECTION = 'mysql://scraper:8ZQ8Mpfu@localhost/katastr_db'  # 'mysql://root@localhost/katastr_db'

# Scraping of LV
# Number of KUs in batch
MAX_KU_IN_BATCH = 8  # 50
# Maximum number of invalid items in a row, which we want to check
MAX_INVALID_ITEMS_IN_ROW = 500

# Scraping of 'rizeni'
# Number of 'rizeni' in batch
MAX_RIZENI_IN_BATCH = 8  # 30
# Number of days in past to scrape 'rizeni'
MAX_DAYS_IN_PAST_TO_SCRAPE_RIZENI = 7

# Scraping of 'stavebni objekty'
# Number of items in batch
MAX_STAVEBNI_OBJEKTY_IN_BATCH = 100
# Number of processes
MAX_STAVEBNI_OBJEKTY_PROCESSES = 8
