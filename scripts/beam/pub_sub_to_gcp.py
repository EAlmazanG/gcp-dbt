import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import json
import ast

class ParseMessage(beam.DoFn):
    def process(self, element):
        try:
            parsed = ast.literal_eval(element.decode('utf-8'))
            yield json.dumps(parsed)
        except Exception as e:
            yield beam.pvalue.TaggedOutput('errors', (element, str(e)))

def run():
    import argparse
    parser = argparse.ArgumentParser()

    parser.add_argument('--project', required=True)
    parser.add_argument('--region', required=True)
    parser.add_argument('--input_topic', required=True)
    parser.add_argument('--output_path', required=True)
    parser.add_argument('--temp_location', required=True)
    parser.add_argument('--runner', default='DataflowRunner')

    args, beam_args = parser.parse_known_args()

    options = PipelineOptions(beam_args)
    options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=options) as p:
        messages = (
            p
            | 'Read from PubSub' >> beam.io.ReadFromPubSub(topic=args.input_topic)
            | 'Parse and Convert to JSON' >> beam.ParDo(ParseMessage())
            | 'Write to GCS' >> beam.io.WriteToText(args.output_path, file_name_suffix='.json')
        )