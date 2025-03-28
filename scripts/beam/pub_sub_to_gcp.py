import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import json

class ParsePubSubMessage(beam.DoFn):
    def process(self, element):
        data = element.decode("utf-8").replace("'", '"')
        parsed = json.loads(data)
        yield parsed

def run():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_topic')
    parser.add_argument('--output_path')
    args, beam_args = parser.parse_known_args()

    options = PipelineOptions(beam_args)
    options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=options) as p:
        (
            p
            | 'Read from PubSub' >> beam.io.ReadFromPubSub(topic=args.input_topic)
            | 'Parse message' >> beam.ParDo(ParsePubSubMessage())
            | 'Write to GCS' >> beam.io.WriteToText(args.output_path, file_name_suffix=".json")
        )

if __name__ == '__main__':
    run()