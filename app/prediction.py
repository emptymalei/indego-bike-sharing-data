import argparse
import json
import logging
import os

import pandas as pd
from sklearn import metrics, utils
from sklearn.model_selection import GridSearchCV, train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import LabelEncoder, StandardScaler
from sklearn.tree import DecisionTreeRegressor

from rideindego.helpers.preprocessing import MultiColumnLabelEncoder
from rideindego.parse_config import get_config as _get_config

_CONFIG = _get_config()

logging.basicConfig()
logger = logging.getLogger("prediction")


_COI = [
    "passholder_type",
    "bike_type",
    "trip_route_category",
    "hour",
    "weekday",
    "month",
]

_COT = ["duration"]


def check_null_columns(df_inp):
    """Check if there are null columns in a dataframe"""

    res = {}
    for i in df_inp.columns:
        res[i] = df_inp[i].isnull().any()

    return res


def load_dataframe(data_path):
    """Load dataframe and select columns"""

    df_input = pd.read_csv(data_path)

    df_input["date"] = pd.to_datetime(df_input.date)

    df_input = df_input[_COI + _COT]

    return df_input


def get_encoders(df_inp):

    encoders_obj = MultiColumnLabelEncoder(columns=_COI)
    encoders_obj.fit_transform(df_inp)
    dt_encoders = encoders_obj.encoders

    return dt_encoders


def calculate_model(data_path):
    """Build model"""

    logger.info(f"Loading data from: {data_path}")

    df = load_dataframe(data_path)

    logger.info(check_null_columns(df))

    scaler = StandardScaler()

    X = df[_COI]
    y = df[_COT]
    y = scaler.fit_transform(y)

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    # Calculate encoders
    dt_encoders = get_encoders(df)

    dt_regressor = GridSearchCV(
        DecisionTreeRegressor(random_state=42),
        cv=3,
        param_grid={"max_depth": [1, 2, 3, 4, 5, 6, 7, 8]},
    )

    pipeline_steps = [
        ("encoding", MultiColumnLabelEncoder(columns=_COI, encoders=dt_encoders)),
        ("dt_classifier", dt_regressor),
    ]

    model = Pipeline(pipeline_steps)

    model.fit(X_train, y_train)

    score = model.score(X_test, y_test)

    return {
        "model": model,
        "score": score,
        "best_params": dt_regressor.best_params_,
        "encoders": dt_encoders,
        "scaler": scaler,
    }


def main():
    """"""

    parser = argparse.ArgumentParser(description="model for rider sharing")

    parser.add_argument(
        "-v",
        "--verbose",
        help="Increase output verbosity",
        action="store_const",
        const=logging.DEBUG,
        default=logging.INFO,
    )

    parser.add_argument(
        "-f", "--features", dest="features", help="Input the values of features"
    )

    data_path = _CONFIG.get("etl", {}).get("trip_data", {}).get("combined_data_path")
    data_path = f"{os.sep}" + f"{os.sep}".join(data_path)

    args = parser.parse_args()
    logger.setLevel(args.verbose)
    features = args.features
    if not features:
        logger.error("Please specify features using --features or -f")
    else:
        features = json.loads(features)
    list_of_given_features = set(features.keys())
    if not (list_of_given_features and set(_COI)) == set(_COI):
        logger.error(f"Please specify enough features: {_COI}")

    logger.info("Predicting features: {}".format(features))

    model_result = calculate_model(data_path)

    logger.info(
        "best parameters: {}".format(model_result.get("get_params")),
        "score: {}".format(model_result.get("score")),
    )

    # encoders = model_result.get('encoders')
    print("features", features, "type: ", type(features))
    df_features = pd.DataFrame.from_records([features])
    # for i in df_features.columns:
    #     df_features[i] = df_features[i].apply(
    #         lambda x: encoders.get(i).transform([x])[0]
    #         )

    model = model_result.get("model")
    model_predict = model_result.get("scaler").inverse_transform(
        model.predict(df_features)
    )
    logger.info("predicted duration: {}".format(model_predict))


if __name__ == "__main__":

    main()

    print("END OF GAME")
