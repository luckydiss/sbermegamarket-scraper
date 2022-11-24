import random
import os
# useragents = open('useragents.txt').read().split('\n')
# proxies = open('proxies.txt').read().split('\n')
chromedrivers = os.listdir('undetected_chromedrivers')


def choose_driver():
    driver = f"undetected_chromedrivers/{random.choice(chromedrivers)}"
    return driver


