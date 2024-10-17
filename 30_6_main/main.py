import pandas as pd
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer

from sklearn.model_selection import cross_val_score
from sklearn.preprocessing import OneHotEncoder, FunctionTransformer
from sklearn.preprocessing import StandardScaler

from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.svm import SVC
from statsmodels.tools import categorical


def filter_data(df):
   columns_to_drop = [
       'id',
       'url',
       'region',
       'region_url',
       'price',
       'manufacturer',
       'image_url',
       'description',
       'posting_date',
       'lat',
       'long'
   ]

   # Возвращаем копию датафрейма, inplace тут делать нельзя!
   return df.drop(columns_to_drop, axis=1)


def main():
    df = pd.read_csv('data/homework.csv').drop('id', axis=1)
    X = df.drop('price_category', axis=1)
    y = df['price_category'].apply(lambda x: -1.0 if x=='low' else 0 if x=='medium' else 1)

    prefilter = Pipeline(steps=[
        ('filter', FunctionTransformer(filter_data))
    ])
    # Сохраним в переменную numerical имена всех числовых признаков нашего датасета
    numerical_feats = X.select_dtypes(include=['int64', 'float64']).columns

    # Сохраним в переменную categorical имена всех категориальных признаков нашего датасета
    categorical_feats = X.select_dtypes(include=['object']).columns

    # В категориальных фичах заменяем пропуски модой
    categorical_transformer = Pipeline(steps = [
    ('imputer', SimpleImputer (strategy='mode')),
    ('encoder', OneHotEncoder (handle_unknown='ignore'))
    ] )

    # В численных фичах заменяем пропуски медианой
    numerical_transformer = Pipeline (steps =  [
        ('imputer', SimpleImputer (strategy='median')),
        ('scaler', StandardScaler())
    ] )

    preprocessor = ColumnTransformer (transformers=[
        ('numerical', numerical_transformer, numerical_feats),
        ('categorical', categorical_transformer, categorical_feats)
    ])
    print(preprocessor)
    #print(f'pipeline')  # Press Ctrl+F8 to toggle the breakpoint.
    #df_filtered = filter_data(df).copy()
    #print(df_filtered.columns)




# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
