## What

I was sick of having to duckduckgo "what is redis on AWS called?". Thus this project exists.

## Formats

I wanted to use protobuf and json. If you want others, please fork it and put in a pull request, but for now, I have the following:

1. JSON
2. Parquet
3. Avro
4. CSV

Protobuf ended up being overkill for now, since we don't have that many other facets besides the service and the alternative list.
