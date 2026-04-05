import boto3
from sklearn.feature_extraction.text import TfidfVectorizer
import joblib
import pandas as pd
from time import time
import os
import re
import io

s3_client = boto3.client('s3')
tmp = '/tmp/'
cleanup_re = re.compile('[^a-z]+')


def cleanup(sentence):
    sentence = sentence.lower()
    sentence = cleanup_re.sub(' ', sentence).strip()
    return sentence


def main(args):
    endpoint_url = args.get('endpoint_url', 'http://172.27.117.185:9000')
    aws_access_key_id = args.get('aws_access_key_id', 'minioadmin')
    aws_secret_access_key = args.get('aws_secret_access_key', 'minioadmin')
    
    # 创建带认证的 s3_client
    s3_client = boto3.client('s3',
        endpoint_url=endpoint_url,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )

    x = args['x']

    dataset_object_key = args['dataset_object_key']
    dataset_bucket = args['dataset_bucket']

    model_object_key = args['model_object_key']  # example : lr_model.pk
    model_bucket = args['model_bucket']

    model_path = tmp + model_object_key
    if not os.path.isfile(model_path):
        s3_client.download_file(model_bucket, model_object_key, model_path)

    # dataset_path = 's3://'+dataset_bucket+'/'+dataset_object_key
    # dataset = pd.read_csv(dataset_path)
    # 使用 s3_client 获取文件内容
    obj = s3_client.get_object(Bucket=dataset_bucket, Key=dataset_object_key)
    dataset = pd.read_csv(io.BytesIO(obj['Body'].read()))

    start = time()

    df_input = pd.DataFrame()
    df_input['x'] = [x]
    df_input['x'] = df_input['x'].apply(cleanup)

    dataset['train'] = dataset['Text'].apply(cleanup)

    tfidf_vect = TfidfVectorizer(min_df=100).fit(dataset['train'])

    X = tfidf_vect.transform(df_input['x'])

    model = joblib.load(model_path)
    y = list(model.predict(X))

    latency = time() - start
    os.remove(dataset_path)
    os.remove(model_path)
    return {'y': str(y), 'latency': latency}
