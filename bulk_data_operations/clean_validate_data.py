"""
Clean Validate

Reads in raw .json files.
Routes to transformer.
Routes to validator.
If passes, saves data in defined chunks.
"""
tables = [
    'business',
    'user',
    ''
]

def route_data(filename):
    table_name = get_source_from_name(filename)

def get_source_from_name(filename):


def tranform_XXX():
    pass

def validate_XXX():
    pass

def save_chunks(data, max_size, prefix):
    pass