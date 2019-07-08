## How do I improve it


### ETL

1. Transformation should be done using line separated json instead of pandas. Line separated json will make it more robust. Pandas transformations fail on one single line of error.

### Model

1. Should consider the following factors:
    1. Weather: rain, temperature;
    2. Geolocations: I created a geolocation tool using OSM data.
    3. Holiday data
2. Better features