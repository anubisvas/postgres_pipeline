import joblib
import pandas as pd
from sklearn.neural_network import MLPClassifier
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer, make_column_selector, make_column_transformer
from sklearn.impute import SimpleImputer

from sklearn.model_selection import cross_val_score
from sklearn.preprocessing import OneHotEncoder, FunctionTransformer
from sklearn.preprocessing import StandardScaler

from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.svm import SVC
#from statsmodels.tools import categorical


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
   df = df.drop(columns_to_drop, axis=1)
   return df
   # Возвращаем копию датафрейма, inplace тут делать нельзя!

def year_filter (df):
    q25 = df['year'].quantile(0.25)
    q75 = df['year'].quantile(0.75)
    iqr = q75 - q25
    boundaries = (q25 - 1.5 * iqr, q75 + 1.5 * iqr)
    df.loc[df['year'] < boundaries[0], 'year'] = round(boundaries[0])
    df.loc[df['year'] > boundaries[1], 'year'] = round(boundaries[1])
    return df

def feature_engine(df):
    def short_model(x):
        if not pd.isna(x):
            return x.lower().split(' ')[0]
        else:
            return x

    # Добавляем фичу "short_model" – это первое слово из колонки model
    df['short_model'] = df['model'].apply(short_model)
    # Добавляем фичу "age_category" (категория возраста)
    df['age_category'] = df['year'].apply(lambda x: 'new' if x > 2013 else ('old' if x < 2006 else 'average'))
    return df

# def drop_untransformed_columns(df):
   # columns_to_drop = [
   #     'year',
   #     'model',
   #     'fuel',
   #     'odometer',
   #     'title_status',
    #    'transmission',
   #     'state',
   #     'short_model',
   #     'age_category'
   # ]

   # df.drop(columns_to_drop, axis=1)
   # return df

def transform_columns(df):
    df = filter_data(df)
    df = year_filter(df)
    df = feature_engine(df)
    return df

def main():
    df = pd.read_csv('data/homework.csv')
    #preprocessor1 = Pipeline(steps=[
     #   ('filter', FunctionTransformer)
    #]) # поставим функцию удаления колонок тут

    X = df.drop('price_category', axis=1)
    y = df['price_category'].apply(lambda x: -1.0 if x == 'low' else 0 if x == 'medium' else 1)


    preprocessor = make_column_transformer(
        (Pipeline(steps=[
            ('imputer', SimpleImputer(strategy='median')),
            ('scaler', StandardScaler())
             ]), make_column_selector(dtype_include=['int64', 'float64'])), # В численных фичах заменяем пропуски медианой
        (Pipeline(steps=[
            ('imputer', SimpleImputer(strategy='most_frequent')),
            ('encoder', OneHotEncoder(handle_unknown='ignore'))
        ]), make_column_selector(dtype_include='object')) # В категориальных фичах заменяем пропуски модой
    )

    preprocessor_feat = Pipeline(steps=[
        ('drop_filter', FunctionTransformer(transform_columns)),
        ('preprocessor', preprocessor),
        #('drop_function', FunctionTransformer(drop_untransformed_columns))
    ])
    #print(f'pipeline')  # Press Ctrl+F8 to toggle the breakpoint.
    #df_filtered = filter_data(df).copy()
    #print(df_filtered.columns)
    models = (
        LogisticRegression(solver='liblinear'),
        RandomForestClassifier(),
        MLPClassifier(activation='logistic', max_iter=300) # , hidden_layer_sizes=(100, 20)
    )
    best_score = .0
    best_pipe = None
    for model in models:
        pipe = Pipeline(steps=[
            ('preprocessor_feat', preprocessor_feat),
            ('classifier', model)
        ])
        score = cross_val_score(pipe, X,y , cv=4, scoring='accuracy')
        print(f'model: {type(model).__name__}, acc_mean: {score.mean():.4f}, acc_std: {score.std():.4f}')

        if score.mean() > best_score:
            best_score = score.mean()
            best_pipe = pipe

    print(f'best model: {type(best_pipe.named_steps["classifier"]).__name__}, accuracy: {best_score:.4f}')
    joblib.dump(best_pipe, 'vehicle_pipe.pkl')



# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
