# Indego bike sharing

Indego bike sharing data

## What is included

1. `app` contains the main code
2. `notebooks` are my experiments


Details

```
.
├── README.md               # this readme file
├── app                     # main code is here
│   ├── config              # config files
│   ├── get_indego_data.py  # RUN this to get the data from data source
│   ├── prediction.py       # RUN this with params to get the predictions
│   └── rideindego          # utility functions
├── notebooks                             # My experimental notebooks
│   ├── etl.ipynb                         # experiments on ETL
│   ├── external-data-enhancing.ipynb     # Didn't have time to do this: enhance data using external data
│   ├── playground                        # Just a playground nothing useful
│   ├── tree_regressor.ipynb             # testing decision tree regressor
│   └── wrangling_and_explorations.ipynb  # data wrangling and EDA
├── notes
│   └── improvements.md
└── requirements.txt
```



## Usage

Specify parametesr in `config/rideindego.yml`

Create environment `conda create --name indego pip`

Install requirements: `pip install -r requirements.txt`

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