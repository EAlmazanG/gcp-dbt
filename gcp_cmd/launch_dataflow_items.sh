gcloud dataflow flex-template run stream-items-to-gcs \
  --project=gcp-dbt-454911 \
  --region=europe-southwest1 \
  --template-file-gcs-location=gs://dataflow-templates/latest/flex/python \
  --parameters=input_topic=projects/gcp-dbt-454911/topics/items-stream,output_path=gs://gcp-dbt_datalake/raw/streaming/items/output,python_file=gs://gcp-dbt_datalake/scripts/beam/pubsub_to_gcs.py \
  --additional-experiments=use_runner_v2 \
  --disable-public-ips \
  --max-workers=2 \
  --worker-machine-type=n1-standard-1
