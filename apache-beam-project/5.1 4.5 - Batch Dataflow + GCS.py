import apache_beam as beam
import os
from apache_beam.options.pipeline_options import PipelineOptions

pipeline_options = {
    'project': 'gcp-airflow-323708' ,
    'runner': 'DataflowRunner',
    'region': 'europe-west2',
    'staging_location': 'gs://ml6-code-challenge/temp',
    'temp_location': 'gs://ml6-code-challenge/temp',
    'template_location': 'gs://ml6-code-challenge/template/batch_job_df_gcs_state' 
    }
    
pipeline_options = PipelineOptions.from_dictionary(pipeline_options)
p1 = beam.Pipeline(options=pipeline_options)

serviceAccount = "gcp-airflow-323708-01e238696a2e.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= serviceAccount

class Filter(beam.DoFn):
  def process(self,record):
    if int(record[1]) > 17:
      return [record]

legal_age = (
p1
  | "Import Data time" >> beam.io.ReadFromText("gs://ml6-code-challenge/input/data.csv", skip_header_lines = 1)
  | "Split by comma time" >> beam.Map(lambda record: record.split(','))
  | "Filter age " >> beam.ParDo(Filter())
  | "Create a key-value State" >> beam.Map(lambda record: (record[4],int(record[1])))
  | "Sum by key State" >> beam.CombinePerKey(sum)
#  | "Print Results" >> beam.Map(print)
)


state_num = (
    p1
    | "Import Data" >> beam.io.ReadFromText("gs://ml6-code-challenge/input/data.csv", skip_header_lines = 1)
    | "Split by comma" >> beam.Map(lambda record: record.split(','))
    | "Filter Age" >> beam.ParDo(Filter())
    | "Create a key-value" >> beam.Map(lambda record: (record[4],int(record[1])))
    | "Count by key" >> beam.combiners.Count.PerKey()
#    | "Print Results" >> beam.Map(print)
)

Delay_table = (
    {'State_num':state_num,'legal_age':legal_age} 
    | "Group By" >> beam.CoGroupByKey()
    | "Save to GCS" >> beam.io.WriteToText("gs://ml6-code-challenge/output/state_output.csv")
    #| beam.CoGroupByKey()
    #| beam.Map(print)
)


p1.run()