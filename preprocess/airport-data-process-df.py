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

    # Replace missing values with empty string
    df.fillna("", inplace=True)
    # Ignore field
    df.elevation_ft = '0'

    print(f"Count '{df.count()}'")
    # Generate JSON files
    for idx, group in df.groupby(np.arange(len(df))//10000):
        f = output_folder + f'airport_code_{idx}.json'
        with open(f, 'w') as json_file:
            print(f)
            for _, r in group.iterrows():
                r.to_json(json_file); json_file.write('\n')



def main():
    """
    Main method
    """
    input_file = '../data/airport-codes_csv.csv'
    output_folder = '../data/'

    process(input_file, output_folder)

if __name__ == "__main__":
    main()

