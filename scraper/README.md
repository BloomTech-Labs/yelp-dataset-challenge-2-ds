# Yelp Scraper

An intelligent API access framework to pull large amounts of data over a geographic area with limited API output.

**Features** include:

* API wrapper in consolidated scraper class that can be called with city name or centroid coordinates.

* Predictive pathing adjustment to control scraper traversal in geographic region to help minimize get requests, maximize returns.

## Usage

### Making Scrape Request

## Updates

*Version Information*

> 2019-11-22 - 0.1 Pre-Release

## Testing

Not Implemented Yet

### Running the test script

> Navigate to ...
> Run test scripts with: python tests.py

## Notes

### Decimal Degrees - Scaling

From Wikipedia: https://en.wikipedia.org/wiki/Decimal_degrees

> Minimum Jump: **0.001** Approximately a neighborhood, streed (~111m)
> Minimum Model Radius: **0.01** Approximately a town or village (~1.11km)
> Maximum Model Radius: **0.05** Half a city or district (~5.55km)

### Logging

Logging is enabled by default at the INFO level.  Logs are written to instance/logs/debug.log.  You can configure this or write to external source, console by modifying basic config.

## Deploying

Deployment notes

#### Special Thanks

Selenium