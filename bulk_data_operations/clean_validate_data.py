"""
Clean Validate

Reads in raw .json files.
Routes to transformer.
Routes to validator.
If passes, saves data in defined chunks.
"""
import pandas as pd
from awstools import s3


table_transformers = {
    'businesses': tvf_business,
    'users': tvf_user,
    'checkins': tvf_checkin,
    'photos': tvf_photo,
    'tips': tvf_tips,
    'reviews': tvf_review,
}


def route_data(filename):
    # Check filename for tablename and execute transform/validate function
    table_name = get_source_from_name(filename)
    try:
        table_transformers[table_name](filename)
        return True
    except:
        print('Could not run transformer')
        raise
    return False


def get_source_from_name(filename):
    for table_name in table_transformers.keys():
        if table_name in filename:
            return table_name
    raise NameError('Tablename not found.  Aborting.')


def load_data(filename):
    # Check file type and load data
    data = None
    return data


def write_data(data, savename, max_size, prefix):
    print('Data received')


def save_chunks(data, max_size, prefix):
    pass


##################################
###Transform Validate Functions###
##################################

def tvf_review(filename):
    data = load_data(filename)
    write_data(data, savename='review', max_size=10000, prefix='clean')
    return True


if __name__ == "__main__":
    load_data(filename)