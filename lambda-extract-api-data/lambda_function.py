import requests
import json
import datetime
import os 
import boto3
import awswrangler as wr


def lambda_handler(event, context):
    target_bucket = 'raw-data-bucket'
    s3_lookup_table_path = 's3://processed-data-bucket/dim_locations/lookup_locations.csv'
    past_datetime = datetime.datetime.now() - datetime.timedelta(days = 1 )
    past_date = past_datetime.strftime("%Y-%m-%d")
    target_object = f'air_quality_dataset/date={past_date}'
    s3_client = boto3.client('s3')
    df_lookup_table = wr.s3.read_csv(path = s3_lookup_table_path, dtype=str)
    lookup_locations = df_lookup_table.to_dict(orient='records')
    latitude_list = df_lookup_table.latitude.tolist()
    longitude_list = df_lookup_table.longitude.tolist()
    latitude_items = ",".join(latitude_list).strip()
    longitude_items = ",".join(longitude_list).strip()
    air_quality_url = f'https://air-quality-api.open-meteo.com/v1/air-quality?latitude={latitude_items}&longitude={longitude_items}&hourly=european_aqi,european_aqi_pm2_5,european_aqi_pm10,european_aqi_nitrogen_dioxide,european_aqi_ozone,uv_index&past_days=1&forecast_days=0&timeformat=unixtime'
    air_quality_response = requests.get(air_quality_url)
    air_quality_response_json = air_quality_response.json()
    for location in air_quality_response_json: 
        latitude = location['latitude']
        longitude = location['longitude']
        required_location = [location for location in lookup_locations if str(location['latitude'].strip()) == str(latitude) and str(location['longitude'].strip()) == str(longitude)]
        if len(required_location) == 0:
            raise ValueError(f'Unable to find the location ID - {str(location)}')
        location_id = required_location[0]['location_id']
        s3_client.put_object(Bucket = target_bucket, 
        Body = json.dumps(location).encode('utf-8'),
        Key = f'{target_object}/location_{str(location_id)}.json')
    return {
        'statusCode': 200,
        'body': json.dumps(f'Air Quality Dataset Dated - {past_date} is uploaded to S3 Bucket!')
    }
