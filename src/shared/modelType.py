from enum import Enum, unique

@unique
class ModelType(Enum):
    '''
        An enum which enumerates the different models we can run for the
        data.
    '''
    __order__ = 'placeholder'
    placeholder = 1

    def __str__(self):
        if self is ModelType.placeholder:
            return "placeholder"

    #     elif self is DataSource.injuries:
    #         return "injuries"
    #     elif self is DataSource.pgpd:
    #         return "pgpd"
    #     elif self is DataSource.pgtd:
    #         return "pgtd"
    #     else:
    #         raise ValueError("Invalid argument for DataSource.__str__(self)")
