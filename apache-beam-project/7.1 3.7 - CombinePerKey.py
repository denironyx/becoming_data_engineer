import apache_beam as beam

p1 = beam.Pipeline()

State_num = (
p1
  | "Import Data" >> beam.io.ReadFromText("data.csv", skip_header_lines = 1)
  | "Split by comma" >> beam.Map(lambda record: record.split(','))
  | "Filter Delays" >> beam.Filter(lambda record: int(record[1]) > 18)
  | "Create a key-value" >> beam.Map(lambda record: (record[4],int(record[1])))
  | "Sum by key" >> beam.CombinePerKey(sum)
  | "Print Results" >> beam.Map(print)
)

p1.run()