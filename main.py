import requests
from bs4 import BeautifulSoup
from lxml import etree
import xlsxwriter

import pandas as pd
import numpy as np

import asyncio
import aiohttp

import nest_asyncio
nest_asyncio.apply()

def get_detail_urls(source, chartid, quantity=500):
    source_url = f'https://{source}/Charts/Index?chartId={chartid}'
    detail_urls = []

    webpage = requests.get(source_url)
    soup = BeautifulSoup(webpage.content, "html.parser")
    dom = etree.HTML(str(soup))
    for x in range(quantity):
        href = dom.xpath('//span[@class="name_1"]/a')[x].attrib["href"].replace("\t", "")
        url = f'https://{source}{href}'
        nganh_nghe = dom.xpath('//span[@class="col-xs-12 col-sm-6 nganh-nghe"]/span/a')[(x-1)*2+1].text
        detail_urls.append((url, nganh_nghe))

    return detail_urls

async def crawl_info(url, ord, nganh_nghe, infos):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            webpage = await response.text()

    dom = etree.HTML(webpage)

    ten_cty = dom.xpath('//div[@class="more_info"]/h2/span')[0].text
    rank = ord+1
    info = [ten_cty, rank, nganh_nghe]
    for x in range(2,10):
        try:
            x_path = f'//div[@class="more_info"]/table/tbody/tr[{x}]/td[2]'
            ele = dom.xpath(x_path)[0].text
        except:
            ele = ""
        finally:
            info.append(ele)
    infos.append(info)

async def crawl(indos):
    tasks = [crawl_info(url, i, nganh_nghe, infos) for i, (url, nganh_nghe) in enumerate(detail_urls)]
    await asyncio.gather(*tasks)

def write_to_excel(excel_file_name, infos):
    my_array = np.array(infos, dtype=object)
    df = pd.DataFrame(my_array, columns = ['Tên công ty', 'Xếp hạng', 'Ngành nghề', 'Mã số thuế', 'Mã chứng khoán', 'Trụ sở chính', 'Tel', 'Fax', 'E-mail', 'Website', 'Năm thành lập'])
    workbook = xlsxwriter.Workbook('excel_file_name')
    worksheet = workbook.add_worksheet()
    
    # format num
    bold = workbook.add_format({'bold': True})
    
    
    # viet dataframe ra file excel
    list_of_columns = df.columns.values
    for col in range(len(list_of_columns)):
        worksheet.write(0, col, list_of_columns[col], bold)
    
        # col size
        col_size = 0
        for row in range (len(df)):
            cell = df[list_of_columns[col]][row]
            if type(cell) == str:
                cell = cell.strip()
            col_size = max(col_size, len(str(cell)))
    
            worksheet.write(row+1, col, cell)
        # resize col
        if col_size+3 < len(list_of_columns[col]):
            col_size = len(list_of_columns[col])
        else:
            if not (type(cell) == str):
                # tieng viet co dau nen +3 size cho de nhin
                col_size += 3
        worksheet.set_column(col, col, col_size)
    
    workbook.close()

if __name__ == "__main__":
    detail_urls = get_detail_urls("profit500.vn", 12, 500)
    infos = []
    asyncio.run(crawl(infos))
    write_to_excel("profit500_companies.xlsx", infos)
