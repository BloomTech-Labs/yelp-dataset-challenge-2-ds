"""
Export Transform

    Pull available data from scraper database and format for external processing.
"""

import pandas as pd
from awstools import write_data, generate_job
from read_query import dump_businesses


### Base Class ###

class Exporter():
    def __init__(self):
        pass
    
    def fetch(self):
        NotImplemented
    
    def transform(self):
        NotImplemented
    
    def save_s3(self, tdata: pd.DataFrame, savepath):
        write_data(
            savepath = savepath,
            filetype = 'json',
            dry_run = False,
        )
        self.generate_job()
        
    def export(self):
        pass
        
    def generate_job(self, savepath, job_type, tablename, **kwargs):
        generate_job(
            obectpath = savepath,
            job_type = job_type,
            tablename = tablename,
            **kwargs
        )
    
    def fetch_transform(self):
        return self.transform(
                data = self.fetch()
            )
        
    
    
### Derived Classes ###

class BusinessExport(Exporter):
    def __init__(self):
        super().__init__()
        
    def fetch(self, columns=[
                            'business_id', 'name', 'address', 'city', 'state',
                            'latitude', 'longitude', 'postal_code', 'review_count', 'stars', 
                            'is_open', 'categories']):
        self.data_ = dump_businesses()
        return pd.DataFrame(self.data_, columns=columns)
    
    def transform(self, data=None):
        if data is None and self.data_:
            self.transformed_data = self.data_
            return self.data_
        elif data is not None:
            self.transformed_data = data
            return data # Null transform returns data
        else:
            raise ValueError
        
    def export(self, filename, data=None):
        if data is not None:
            data_to_export = data
        else:
            data_to_export = self.transformed_data
        data_to_export.to_json(orient='records',path_or_buf=filename)