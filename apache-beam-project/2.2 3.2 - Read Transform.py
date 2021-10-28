import apache_beam as beam

p1 = beam.Pipeline()

voos = (
p1
  # Read files
  | "Import Data" >> beam.io.ReadFromText("df_coin_data.csv", skip_header_lines = 1)
  | "Split by comma" >> beam.Map(lambda record: record.split(','))
  | "Print Results" >> beam.Map(print)
)

p1.run()
