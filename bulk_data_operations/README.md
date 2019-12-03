# Bulk Data Operations

This library contains modules to partition large files and write data to the database.

## Database Writer (write_on_job.py)

Writing to the database can be done via direct POST request to the database application; however, large (>100 rows) synchronous uploads are not sugguested.  The database api will attempt to validate foreign keys, creating blanks where necessary.  This can mean up to 3*n_rows or greater SELECT, INSERT, and UPDATE statements and may be slow.  After so long, Flask will kill non-responding workers or Heroku will cut the stale client connection.  No keep-alive funcationality has been built as of V1.0.

### Usage of write_on_job

#### SETUP

#### PARAMS

#### LOGGING