# user-insights-pipeline

## Build flex template

```
gcloud dataflow flex-template build gs://pt-dataflow-bucket/pt-templates/user-insights.json \
  --image-gcr-path=us-east1-docker.pkg.dev/ptransformers/pt-pipelines/user-insights:latest \
  --jar=/home/ganesh/ptransformers/userpipeline/user-insights-pipeline/userinsights/target/userinsights-1.0.jar \
  --env=FLEX_TEMPLATE_JAVA_MAIN_CLASS=com.ptransformers.App \
  --flex-template-base-image=JAVA17 \
  --sdk-language=JAVA
```

## Run template

```
gcloud dataflow flex-template run "user-insights-test" \
  --template-file-gcs-location="gs://pt-dataflow-bucket/pt-templates/user-insights.json" \
  --service-account-email="runtime@ptransformers.iam.gserviceaccount.com" \
  --staging-location="gs://pt-dataflow-bucket/staging/" \
  --temp-location="gs://pt-dataflow-bucket/temp/" \
  --network="default" \
  --subnetwork="https://www.googleapis.com/compute/v1/projects/ptransformers/regions/us-east1/subnetworks/default" \
  --region="us-east1" \
  --project="ptransformers" \
  --impersonate-service-account="runtime@ptransformers.iam.gserviceaccount.com"
```
