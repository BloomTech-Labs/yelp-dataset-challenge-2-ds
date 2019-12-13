"""
Clean Validate

Reads in raw .json files.
Routes to transformer.
Routes to validator.
If passes, saves data in defined chunks.
"""
import pandas as pd
import s3
from math import ceil
import os
from io import BytesIO
import json
from jobs import generate_job


class g():
    def __init__(self, bucket=None):
        self.bucket=bucket

g = g()


############################
###Data and File Handling###
############################

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


def get_bucket():
    if g.bucket == None:
        bucket = s3.Bucket('yelp-data-shared-labs18')
        g.bucket = bucket
        return g.bucket
    else:
        return g.bucket


def get_source_from_name(filename):
    for table_name in table_transformers.keys():
        if table_name in filename:
            return table_name
    raise NameError('Tablename not found.  Aborting.')


def load_data(filename):
    """Load Data
        Loads data into Pandas DataFrame

        param filename: full filepath + filename or s3 bucket object name.
        type filename: string
        returns: DataFrame
    """
    filetype = filename.split('.')[-1]
    print('Detected {} file.'.format(filetype))

    if filetype == 'json':
        data = pd.read_json(filename)
    elif filetype == 'csv':
        data = pd.read_csv(filename)
    return data


def write_data(data, savepath, dry_run=True, filetype='parquet'):
    print('Saving {}'.format(savepath))
    if dry_run:
        print('Executing Dry Run to {}'.format(savepath))
        file_stream = BytesIO()
        data.to_parquet(file_stream)
        print(pd.read_parquet(file_stream).head())
    else:
        print('Commencing upload of {} to S3'.format(savepath))
        tempfilename = '/tmp/'+savepath.split('/')[-1]
        if filetype == 'parquet':
            data.to_parquet(tempfilename)
        elif filetype == 'json':
            data.to_json(tempfilename, orient='records')
        else:
            raise TypeError("Only parquet or json saving supported")
        bucket = get_bucket()
        bucket.save(tempfilename, savepath)
        os.remove(tempfilename)


def save_chunks(data, max_size, prefix, rootname, path, filetype='parquet'):
    """Save Chunks
        Bin dataframe and save into parquet files at defined
        max_size intervals.

        param data: data to bin and save
        type data: dataframe
        param max_size: maximum number of rows in each bin
        type max_size: int
        param prefix: prefix for saved files
        type prefix: string
        param rootname: root file name (e.g. photos, reviews, tips)
        type rootname: string
        param path: absolute path excluding filename or s3 object path excluding filename
        type path: string
    """
    # Bin dataframes
    binned_frames = bin_dataframe(data=data, max_size=max_size)
    # Write bins
    savepaths = []
    for count, frame in enumerate(binned_frames):
        savepath = path + prefix + '_' + rootname + '_' + str(count) + '.' + filetype
        savepaths.append(savepath)
        write_data(data=frame, savepath=savepath, dry_run=False, filetype=filetype)
    return savepaths


def bin_dataframe(data, max_size):
    len_data = len(data)
    if len_data >= max_size:
        num_chunks = ceil(len_data/max_size)
    else:
        num_chunks = 1
    print('Preparing {} chunks of size {} for data of len {}'.\
        format(num_chunks, max_size, len_data))
    binned_frames = []
    for i in range(num_chunks):
        start = i*max_size
        stop = (i+1)*max_size-1
        if stop >= len_data:
            binned_frames.append(
                data.loc[start:, :]
            )
        else:
            binned_frames.append(
                data.loc[start:stop, :]
            )
    return binned_frames


##################################
###Transform Validate Functions###
##################################

def tvf_business(filename):
    print('Beginning business data transformation.')
    data = load_data(filename)
    savepaths = save_chunks(
        data=data,
        max_size=30000,
        prefix='clean_transformed',
        rootname='business',
        path='Processed/',
        filetype='json'
        )
    for path in savepaths:
        generate_job(objectpath=path, job_type='POST')


def tvf_user(filename):
    print('Beginning user data transformation.')
    data = load_data(filename)
    savepaths = save_chunks(
        data=data,
        max_size=100000,
        prefix='clean',
        rootname='user',
        path='Clean/',
        )
    for path in savepaths:
        generate_job(objectpath=path, job_type='POST')


def tvf_checkin(filename):
    print('Beginning checkin data transformation.')
    data = load_data(filename)
    data['checkin_id'] = data.apply(generate_id, axis=1)
    data = data.rename(columns={'date':'dates'})
    savepaths = save_chunks(
        data=data,
        max_size=20000,
        prefix='clean',
        rootname='checkin',
        path='Clean/',
        )
    for path in savepaths:
        generate_job(objectpath=path, job_type='POST')


def tvf_photo(filename):
    print('Beginning photo data transformation.')
    data = load_data(filename)
    savepaths = save_chunks(
        data=data,
        max_size=100000,
        prefix='clean',
        rootname='photo',
        path='Clean/',
        )
    for path in savepaths:
        generate_job(objectpath=path, job_type='POST')


def tvf_tips(filename):
    print('Beginning tip data transformation.')
    data = load_data(filename)
    data['tip_id'] = data.apply(generate_id, axis=1)
    savepaths = save_chunks(
        data=data,
        max_size=100000,
        prefix='clean',
        rootname='tip',
        path='Clean/',
        )
    for path in savepaths:
        generate_job(objectpath=path, job_type='NLP')


def tvf_review(filename):
    print('Beginning review data transformation.')
    data = load_data(filename)
    savepaths = save_chunks(
        data=data,
        max_size=50000,
        prefix='clean',
        rootname='review',
        path='Clean/',
        )
    for path in savepaths:
        generate_job(objectpath=path, job_type='NLP')


def tvf_viz2(filename):
    print('Beginning Vizualization 2 transformation.')
    data = load_data(filename)
    savepaths = save_chunks(
        data=data,
        max_size=20000,
        prefix='processed',
        rootname='viz2',
        path='Processed/',
        filetype='json'
        )
    for path in savepaths:
        generate_job(objectpath=path, tablename='viz2', job_type='POST', dry_run=False)


table_transformers = {
    'business': tvf_business,
    'user': tvf_user,
    'checkin': tvf_checkin,
    'photo': tvf_photo,
    'tip': tvf_tips,
    'review': tvf_review,
    'viz2': tvf_viz2,
}

######################
###Helper Functions###
######################

def generate_id(record):
    """Generate ID returns a repeatable hash of a given record.

    param record: python string, list, or dictionary, pandas.series
    type record: string
    """
    import hashlib
    import pandas as pd
    # Convert series to dictionary object for encoding
    if type(record) == pd.Series:
        record = str(record.to_dict())
    else:
        record = str(record)
    # Encode record to bytes
    record = record.encode()
    return hashlib.sha256(record).hexdigest()


if __name__ == "__main__":

    # Example Usage
    # businesses = 'business_transformed.json'
    # route_data(businesses)

    route_data("viz2.json")