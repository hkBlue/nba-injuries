from enum import Enum, unique

@unique
class DataSourceType(Enum):
    '''
        An enum which enumerates the different data sources we can gather
        data from.
    '''
    __order__ = 'injuries pgpd pgtd'
    injuries = 1
    pgpd = 2
    pgtd = 3

    def __str__(self):
        if self is DataSourceType.injuries:
            return "injuries"
        elif self is DataSourceType.pgpd:
            return "pgpd"
        elif self is DataSourceType.pgtd:
            return "pgtd"
        else:
            raise ValueError("Invalid argument.")
