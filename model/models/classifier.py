# Import the modules

import datetime
import logging
import pickle
import sys
import time

import pandas as pd


sys.path.insert(0, "./../")


# Configure the logging level and format
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")



# Define the class
class MultiDimModelCreditMachine:
    """Это класс загрузки и предикта многомерной модели
    """    
    # Initialize the class with a model file name
    def __init__(
        self, model_file="model/models/resources/auto_ml_retrain_local_run.pkl", scaler_file="model/models/resources/multi_dim_predictscaler.pkl", from_disk=True, joined_wide_df_location='model/models/resources/joined_wide_df_20231116_v3.pickle', **kwargs
    ):
        """инициализация

        Args:
            model_file (str): ссылка на pkl модели
            scaler_file (str): ссылка на pkl скейлера
            from_disk (bool): загружать ли данные с диска или делать etl
            joined_wide_df_location (ыек, optional): ссылка на данные joined_wide_df.
        """        
        # Load the model from pickle
        self.loaded_model = pickle.load(open(model_file, "rb"))
        # добавляем внучную свойство которого нет в нашем пикле модели, так как он создан в 0.3.2 а запускаем в 0.3.8
        self.loaded_model.is_time_series = False
        # Load the scaler from pickle
        self.scaler = pickle.load(open(scaler_file, "rb"))
        # Set where to load
        self.local = from_disk
        # Set_df_location
        self.joined_wide_df_location = joined_wide_df_location

        # add none to unused yet
        self.joined_wide_df = None
        # Log a message with the current datetime
        self.additional_arguments = kwargs
        logging.info("Initialized the class")

    # Define a method to load the dataset from a pandas dataframe
    def load(self, dataframe):
        """зазрузка данных
        """        
        if self.local:
            # Read the data file as a pandas dataframe
            self.joined_wide_df = pd.read_pickle(
                self.joined_wide_df_location
            )
            logging.info(f"Loaded the data, from_disk={self.local}")
        # Log a message with the current datetime
        else:
            logging.info(f"Loaded the data from dataframe")
            self.joined_wide_df = dataframe.set_index('timestamp')

    # Define a method to predict the credit score using the model and the data
    def predict(self):
        """predict

        Returns:
            float: вероятность инцидента или аномалии
        """        
        logging.debug('self.joined_wide_df')
        logging.debug(self.joined_wide_df)
        train_data = self.joined_wide_df.loc[
            self.joined_wide_df.index.max()
            - pd.Timedelta("1day")
            - pd.Timedelta("15minute") :
        ]
        dt_tz = pd.DataFrame(
            {
                "timestamp": pd.date_range(
                    pd.Timestamp(train_data.index.min()),
                    pd.Timestamp(train_data.index.max()),
                    freq="min",
                )
            }
        )

        joined_wide_df_no_holes = (
            dt_tz.merge(self.joined_wide_df.reset_index(), how="left", on=["timestamp"])
            .interpolate(limit_direction="backward")
            .interpolate(limit_direction="forward")
        )

        joined_wide_df_no_holes["timestamp"] = [
            pd.Timestamp(t).tz_localize("UTC")
            for t in joined_wide_df_no_holes["timestamp"].values
        ]
        X = joined_wide_df_no_holes.rename(columns={"timestamp": "datetime"})

        X_modified = X[X.columns[1:]]
        X_modified.index = X["datetime"].values

        X_modified = self.scaler.transform(X_modified)

        data = pd.DataFrame(X_modified, columns=X.columns[1:])
        modified_data_v2 = data.copy().set_index(X.datetime)

        modified_X_v2_mean = modified_data_v2.rolling("1D").mean()
        modified_X_v2_std = modified_data_v2.rolling("1D").std()
        modified_X_v2_mean.columns = [i + "_mean" for i in modified_X_v2_mean.columns]
        modified_X_v2_std.columns = [i + "_std" for i in modified_X_v2_std.columns]
        modified_X_v2_saved = modified_data_v2
        modified_X_v2 = (
            modified_X_v2_saved.join(modified_X_v2_mean)
            .join(modified_X_v2_std)
            .iloc[1:]
        )

        values = self.loaded_model.predict(modified_X_v2.tail(1))
        self.predictions = values.data[0][0]

        # Log a message with the current datetime
        logging.info("Predicted values")
        # Return the predictions as a value
        return self.predictions
