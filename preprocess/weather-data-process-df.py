import pandas as pd
import numpy as np


def process(input_file, output_folder):
    """
    Perform data cleanup and conversion to JSON format
    :param input_file: path to CSV file
    :param output_folder: path to folder with generated JSON files
    """
    print(f"Input '{input_file}'")

    df = pd.read_csv(input_file, header=0)

    # Remove rows with missing data
    df.dropna(subset=['City', 'Country', 'AverageTemperature', 'AverageTemperatureUncertainty', 'dt'], inplace=True)
    # Rename columns
    df.rename(columns={"AverageTemperature": "average_temperature", "AverageTemperatureUncertainty": "average_temperature_uncertainty",
                       "City": "city", "Country": "country", "Latitude": "latitude", "Longitude": "longitude"}, inplace=True)

    print(f"Count '{df.count()}'")
    # Generate JSON files
    for idx, group in df.groupby(np.arange(len(df))//10000):
        f = output_folder + f'weather_data_{idx}.json'
        with open(f, 'w') as json_file:
            print(f)
            for _, r in group.iterrows():
                r.to_json(json_file); json_file.write('\n')



def main():
    """
    Main method
    """
    input_file = '../data/GlobalLandTemperaturesByCity.csv'
    output_folder = '../data/'

    process(input_file, output_folder)

if __name__ == "__main__":
    main()

