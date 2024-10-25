import dill
import pandas as pd

def main():
    with open('vehicles.pkl','rb') as file:
        best_pipe = dill.load(file)
    df = pd.read_csv('data/homework.csv')
    row = df.sample(1)
    row_x = row.drop(['price_category'], axis=1)
    row_y = row['price_category']
    pred = best_pipe['model'].predict(row_x)
    print(pred, row_y)
    print(best_pipe['metadata'])

    df = pd.read_csv('data/homework.csv').drop('price_category', axis=1)

    types = {
       'int64': 'int',
       'float64': 'float'
        }
    for k, v in df.dtypes.items():
        print(f'{k}: {types.get(str(v), "str")}')


if __name__ == '__main__':
        main()