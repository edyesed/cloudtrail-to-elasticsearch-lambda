# cloudtrail-to-elasticsearch-lambda
Lambda to take things cloudtrail dropped off in S3, and put them into DynamoDB

It used to drop things off in Elasticsearch, and if you want that, checkout the repo at [this commit](https://github.com/edyesed/cloudtrail-to-elasticsearch-lambda/tree/c14b5e1e3aac11c243f3c53a96f85ee35ae91d1e)

elasticsearch was kind of expensive, and honestly I didn't need anything ES specific for this, I just needed a place to put json

# For local dev
1. I use `python-local-lambda`
