
import datetime, logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import WriteToText
from apache_beam.io.gcp.bigquery import ReadFromBigQuery, WriteToBigQuery

class format_key(beam.DoFn):
    def process(self, element):
    
        state = element['state']
        city = element['city']
        combined_key = element['combined_key']
        

        # fromats key to follow other tables formats
        s,c=combined_key.split(',')
        s=s.replace(' ','_')
        c=c.replace(' ','_')
        combined_key=c+'_'+s
        
        record = {'combined_key': combined_key, 'state': state, 'city': city}
        return [record]   
        
def run():
    PROJECT_ID = 'starlit-vim-303003'
    BUCKET = 'gs://pandas-aj/temp'
    DIR_PATH = BUCKET + '/output/' + datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S') + '/'
    
    options = PipelineOptions(
    flags=None,
    runner='DataflowRunner',
    project=PROJECT_ID,
    job_name='location1',
    temp_location=BUCKET + '/temp',
    region='us-central1')
        
    p = beam.pipeline.Pipeline(options=options)

    sql = 'SELECT * FROM datamart.Location'
    bq_source = ReadFromBigQuery(query=sql, use_standard_sql=True, gcs_location=BUCKET)

    query_results = p | 'Read from BQ' >> beam.io.Read(bq_source)

    out_pcoll = query_results | 'Format Name' >> beam.ParDo(format_key())

    out_pcoll | 'Write Results' >> WriteToText(DIR_PATH + 'output_location.txt')

    dataset_id = 'datamart'
    table_id = PROJECT_ID + ':' + dataset_id + '.' + 'location_Dataflow'
    schema_id = 'state:STRING,city:STRING,combined_key:STRING'

    out_pcoll | 'Write to BQ' >> WriteToBigQuery(table=table_id, schema=schema_id, custom_gcs_temp_location=BUCKET)
     
    result = p.run()
    result.wait_until_finish()      


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.ERROR)
  run()