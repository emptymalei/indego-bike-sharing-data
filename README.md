# Indego bike sharing

Indego bike sharing data

## Usage

Specify parametesr in `config/rideindego.yml`

Fetch data:
```
python app/get_indego_data.py
```

Predictions:

```
python app/prediction.py -f '{"passholder_type":"Indego30","trip_route_category":"One Way","hour":11,"weekday":0,"month":8, "bike_type": "standard"}'
```

## Development

1. create environment `conda create --name indego pip`