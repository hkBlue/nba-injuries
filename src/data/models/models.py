'''
    Contains the Models for each dataSource
    Readers and Writers can interact with these 
    models to gaurantee they behave in the same manner.
'''

class InjuriesModel:
    '''
        Models the data which we put into the Injuries csv
    '''
    def get_as_csv_row(self):
        pass

    def read_from_csv_row(self):
        pass

class PgpdModel:
    '''
        Models the data which we put into the Pgpd csv.
    '''
    def get_as_csv_row(self):
        '''
            Returns a string which is this model 
        '''
        pass    

    def read_from_csv_row(self):
        pass


class PgtdModel:
    '''
        Models the data which we put into the Pgtd csv.
    '''
    def get_as_csv_row(self):
        pass    

    def read_from_csv_row(self):
        pass

