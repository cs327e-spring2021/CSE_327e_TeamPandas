import logging
import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.io.gcp.bigquery import ReadFromBigQuery, WriteToBigQuery

class format_key(beam.DoFn):
    def process(self, element):
    
        state = element['state']
        city = element['city']
        combined_key = element['combined_key']

        # fromats key to follow other tables formats
        combined_key=combined_key.replace(',','_')
        
        record = {'combined_key': combined_key, 'state': state, 'city': city}
        return [record]   
        
def run():
     PROJECT_ID = 'starlit-vim-303003'
     BUCKET = 'gs://pandas-aj/temp'

     options = {
     'project': PROJECT_ID
     }
     opts = beam.pipeline.PipelineOptions(flags=[], **options)

     p = beam.Pipeline('DirectRunner', options=opts)

     sql = 'SELECT * FROM datamart.Location'
     bq_source = ReadFromBigQuery(query=sql, use_standard_sql=True, gcs_location=BUCKET)

     query_results = p | 'Read from BQ' >> beam.io.Read(bq_source)

     out_pcoll = query_results | 'Format Name' >> beam.ParDo(format_key())

     out_pcoll | 'Write Results' >> WriteToText('output_location.txt')

     dataset_id = 'datamart'
     table_id = PROJECT_ID + ':' + dataset_id + '.' + 'location_beam'
     schema_id = 'state:STRING,city:STRING,combined_key:STRING'

     out_pcoll | 'Write to BQ' >> WriteToBigQuery(table=table_id, schema=schema_id, custom_gcs_temp_location=BUCKET)
     
     result = p.run()
     result.wait_until_finish()      


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.ERROR)
  run()