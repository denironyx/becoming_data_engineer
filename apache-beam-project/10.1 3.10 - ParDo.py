import apache_beam as beam

p1 = beam.Pipeline()

class Filter(beam.DoFn):
  def process(self,record):
    if int(record[1]) > 17:
      return [record]

legal_age = (
p1
  | "Import Data time" >> beam.io.ReadFromText("data.csv", skip_header_lines = 1)
  | "Split by comma time" >> beam.Map(lambda record: record.split(','))
  | "Filter age " >> beam.ParDo(Filter())
  | "Create a key-value State" >> beam.Map(lambda record: (record[4],int(record[1])))
  | "Sum by key State" >> beam.CombinePerKey(sum)
#  | "Print Results" >> beam.Map(print)
)


state_num = (
    p1
    | "Import Data" >> beam.io.ReadFromText("data.csv", skip_header_lines = 1)
    | "Split by comma" >> beam.Map(lambda record: record.split(','))
    | "Filter Age" >> beam.ParDo(Filter())
    | "Create a key-value" >> beam.Map(lambda record: (record[4],int(record[1])))
    | "Count by key" >> beam.combiners.Count.PerKey()
#    | "Print Results" >> beam.Map(print)
)

Delay_table = (
    {'State_num':state_num,'legal_age':legal_age} 
    | beam.CoGroupByKey()
    | beam.Map(print)
)


p1.run()