
import apache_beam as beam
import os
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.metrics import Metrics
from apache_beam.metrics.metric import MetricsFilter
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

# 파이프라인 생성
# 파이프라인 옵션객체를 먼저만듦.
options = PipelineOptions()
google_cloud_options = options.view_as(GoogleCloudOptions)
google_cloud_options.project = 'freud-int-200423'
google_cloud_options.job_name = 'dataflow-test-yun-v1'
google_cloud_options.staging_location = 'gs://freud-int-200423/staging'
google_cloud_options.temp_location = 'gs://freud-int-200423/temp'
options.view_as(StandardOptions).runner = 'DataflowRunner'
# 방금생성한 옵션으로 객체를 만드는과정.
p = beam.Pipeline(options=options)
# 텍스트 파일이 pCollection으로 되어 출력됨.
    (p
        | beam.io.ReadFromText('gs://dataflow-samples/shakespeare/kinglear.txt')
        # 행을 분할함 pCollection<string> 
        | 'ExtractWords' >> beam.FlatMap(lambda x: re.findall(r'[A-Za-z\']+', x))
        # count는 PCollection으로 입력받은 고유워드의 카운트횟수 측정
        | beam.combiners.Count.PerElement()
        # 출력파일쓰기에 적합한 인쇄 가능한 문자열로 형식화하는 과정
        | beam.MapTuple(lambda word, count: '%s: %s' % (word, count))
        # pCollection형식화된 문자열을 마지막입력으로사용되고 저장할 버킷 설정
        | beam.io.WriteToText('gs://freud-int-200423/counts.txt')
    )

# 파이프 라인 실행
result = p.run()
