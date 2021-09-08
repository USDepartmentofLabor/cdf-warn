"""Contains state spider lookup function"""

from modules.spiders import states


def get_spider(state):
    """Return Spider class corresponding to two-letter state abbreviation"""
    
    return getattr(states, f'{state}WARNSpider')