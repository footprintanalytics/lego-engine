import pandas
import os
from config import project_config


# address,name,decimals
def main():
    filename = os.path.join(project_config.dags_folder, 'yield_aggregator/cream_finance/temp.csv')
    df = pandas.read_csv(filename, header=0, sep=',')

    list_1 = []
    for index, row in df.iterrows():

        contract_address = row['contract_address'].lower()
        contract_name = row['contract_name'].lower().replace('-', '_')
        detail = contract_name + ',' + contract_address.lower()
        print(detail)



if __name__ == '__main__':
    main()