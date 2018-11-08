import crawler

def check_scraping_batch():
    sb = crawler.ScrapingBatch()
    sb.create()
    sb.save_log()

if __name__ == '__main__':
    check_scraping_batch()
