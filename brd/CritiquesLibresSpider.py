__author__ = 'mouellet'

import scrapy

xpath_root = '//table/tr'

xpath_title = './td[@class="titre"]/text()'
xpath_star = './td[@class="texte"]/img[contains(@name,"etoiles")]/@name'
xpath_siteuid = './td[@class="texte"]/p/a[contains(@href,"uid=")]/@href'
xpath_pseudo = './td[@class="texte"]/p/a[contains(@href,"uid=")]/text()'
#missing datetime of the review, url?,

class CritiquesLibresSpider(scrapy.Spider):
    name = 'critiqueslibres'
    start_urls = ['http://www.critiqueslibres.com/i.php/vcrit/26627?alt=print']


    def parse(self, response):
        tableRows = response.xpath(xpath_root)
        output = []

        for row in tableRows:

            title = row.xpath(xpath_titre).extract()
            star = row.xpath(xpath_star).extract()
            author = row.xpath(xpath_pseudo).extract()
            uid = row.xpath(xpath_siteuid).extract()

            #just for debug on consol!
            print((title, star, author, uid))

