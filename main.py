import undetected_chromedriver as uc
from selenium.webdriver.common.by import By

import os
import logging
from multiprocessing import Pool
from datetime import datetime

import pandas as pd
from bs4 import BeautifulSoup

from drivers import choose_driver

import warnings
warnings.filterwarnings("ignore")

logging.basicConfig(
    filename='errors.log',
    filemode='a',
    level=logging.ERROR,
)


BARCODES = []
price = pd.read_excel(f"new_barcodes/{os.listdir('new_barcodes')[0]}")
for i in price['Unnamed: 4'].values:
    try:
        BARCODES.append(int(i))
    except:
        continue

class Scraper:
    def __init__(self,barcode):

        """ Chromium """
        self.barcode = barcode
        self.options = uc.ChromeOptions()

        self.options.headless = True
        self.options.add_argument("--headless")

    def run(self):
        try:
            self.chrome = uc.Chrome(options=self.options,
                           driver_executable_path=choose_driver())
            self.chrome.maximize_window()

            process = self.barcode
            print(f"[INFO] Обрабатывается штрихкод: {process} | {datetime.now()}")

            url = f"https://sbermegamarket.ru/catalog/?q={self.barcode}"
            self.chrome.get(url)

            breadcrumbs = self.chrome.find_elements(By.CLASS_NAME, "breadcrumb-item")
            breadcrumb_1, breadcrumb_2, breadcrumb_3, breadcrumb_4 = \
                breadcrumbs[0].text, breadcrumbs[1].text, breadcrumbs[2].text, breadcrumbs[3].text

            self.file_name = f"{os.getcwd()}\\cache\\{breadcrumb_1}-{breadcrumb_2}-{breadcrumb_3}-{breadcrumb_4}.xlsx"

            try:
                df_loaded = self.load_df(self.file_name)
                print(f'[INFO] Файл {self.file_name.split("-")[-1]} открыт для чтения...')
            except:
                print(f'[INFO] Файл {self.file_name.split("-")[-1]} открыт для записи...')
                df_loaded = self.create_df(breadcrumb_1, breadcrumb_2, breadcrumb_3, breadcrumb_4)

            self.parse_page(df_loaded)

        except Exception as ex:
            print(ex)
        finally:
            self.chrome.close()
            self.chrome.quit()

    def create_df(self, breadcrumb_1, breadcrumb_2, breadcrumb_3, breadcrumb_4):
        create_df = pd.DataFrame(columns=['Категория 1', 'Категория 2', 'Категория 3', 'Категория 4','Штрихкод','Фото'])
        create_df.to_excel(f'cache/{breadcrumb_1}-{breadcrumb_2}-{breadcrumb_3}-{breadcrumb_4}.xlsx',
                         index=False, encoding='utf-16')
        return create_df

    def load_df(self, df_name):
        load_df = pd.read_excel(df_name, encoding='utf-16')
        return load_df

    def parse_page(self, df):
        soup = BeautifulSoup(self.chrome.page_source, 'html.parser')

        photos = []
        for i in soup.find_all(class_='lazy-img gallery__thumb-img'):
            photos.append(i['src'])

        items_dir = {}
        items_name = [item.text for item in soup.find_all(class_='pdp-specs__item-name')]
        items_value = [item.text for item in soup.find_all(class_='pdp-specs__item-value')]

        for i in range(len(items_name)):
            items_dir[items_name[i]] = items_value[i]

        breadcrumbs = self.chrome.find_elements(By.CLASS_NAME, "breadcrumb-item")
        breadcrumb_1, breadcrumb_2, breadcrumb_3, breadcrumb_4 = \
            breadcrumbs[0].text, breadcrumbs[1].text, breadcrumbs[2].text, breadcrumbs[3].text

        df = df.loc[df.index].append(items_dir,ignore_index=True)
        df.iloc[df.index-1,[0,1,2,3,4,5]] = [breadcrumb_1, breadcrumb_2, breadcrumb_3, breadcrumb_4,
                                             self.barcode, ",".join(photos)]

        print(f'[INFO] Файл {self.file_name.split("-")[-1]} обновлен... ')

        df.to_excel(self.file_name, index=False,encoding='utf-16')

def run_pricess(barcode):
    parser = Scraper(barcode=barcode)
    parser.run()

def main():
    with Pool(4) as process:
        process.map(run_pricess, BARCODES)

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(f'Ошибка чтения страницы. Пожалуйста подождите...\n{e}')