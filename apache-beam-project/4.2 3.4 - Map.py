import apache_beam as beam

p1 = beam.Pipeline()

voos = (
p1
  | "Import Data" >> beam.io.ReadFromText("data.csv", skip_header_lines = 1)
  | "Split by comma" >> beam.Map(lambda record: record.split(','))
  | beam.Map(lambda record: int(record[1]))
  | "Print Results" >> beam.Map(print)
)

p1.run()
