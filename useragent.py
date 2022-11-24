import random

# useragents = open('useragents.txt').read().split('\n')
# proxies = open('proxies.txt').read().split('\n')
useragents = [line.rstrip('\n') for line in open(f'useragents.txt', 'r')]


def chose_agent():
    agent = random.choice(useragents)
    return agent


