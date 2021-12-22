from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.providers.databricks.operators.databricks import DatabricksRun
from airflow.utils.dates import days_ago



load_path_1 = "/Users/freud@mz.co.kr/test"
load_path_2 =  "/Users/freud@mz.co.kr/test2"
pypi_library = [
            { 'pypi' : {
            'package' : 'joblib==0.17.0'
            }
            },
            { 'pypi' : {
                'package' : 'nbformat==5.1.2'
                }
            },
             { 'pypi' : {
                'package' : 'category-encoders==1.2.6' 
                }
            },
             { 'pypi' : {
                'package' : 'cryptography==3.3.1'  
                }
            },
             { 'pypi' : {
                'package' : 'lightgbm==3.0.0'
                }
            },
             { 'pypi' : {
                'package' : 'qgrid==1.3.1'
                }
            },
             { 'pypi' : {
                'package' :  'networkx==2.5'
                }
            },
             { 'pypi' : {
                'package' : 'matplotlib==3.3.4'
                }
            },
             { 'pypi' : {
                'package' : 'statsmodels==0.9.0'  
                }
            },
             { 'pypi' : {
                'package' : 'dill==0.2.5' 
                }
            },
             { 'pypi' : {
                'package' : 'openpyxl==3.0.7' 
                }
            },
             { 'pypi' : {
                'package' : 'seaborn==0.11.0' 
                }
            },
             { 'pypi' : {
                'package' : 'psycopg2==2.8.6'
                }
            },
             { 'pypi' : {
                'package' : 'scikit-learn==0.21.1'
                }
            },
             { 'pypi' : {
                'package' : 'xgboost==1.2.1'
                }
            },
             { 'pypi' : {
                'package' : 'eli5==0.11.0'
                }
            },
             { 'pypi' : {
                'package' : 'pandas-gbq' 
                }
            },
             { 'pypi' : {
                'package' : 'datetime==4.3' 
                }
            },
            { 'pypi' : {
                'package' : 'pyarrow==2.0.0'  
                }
            },
            { 'pypi' : {
                'package' : 'nbmerge==0.0.4'
                }
            },
            { 'pypi' : {
                'package' : 'Cython==0.29.21' 
                }
            }
                    ]
default_args = {
    'owner': 'airflow',
    'email': ['airflow@example.com'],
    'depends_on_past': False,
}

with DAG(
    dag_id='samsung_databricks_operator',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=days_ago(2),
    tags=['Samsung'],
) as dag:



    new_cluster = {
        'spark_version': '7.3.x-scala2.12',
        'node_type_id': 'n2-highmem-8',
        'num_workers': 1,
        'gcp_attributes': {
           'google_service_account' : 'samsungpoc@mfas-poc.iam.gserviceaccount.com'
        },
    }

    second_clust = {
        'spark_version': '7.3.x-scala2.12',
        'node_type_id': 'n1-highmem-4',
        'num_workers': 1,
        'gcp_attributes': {
           'google_service_account' : 'samsungpoc@mfas-poc.iam.gserviceaccount.com'
        },
    }


    notebook_task_params = {
        'new_cluster': new_cluster,
        'notebook_task': {
            'notebook_path': f'{load_path_1}'
        },
    }

    notebook_task_params2 = {
        'new_cluster': second_clust,
        'notebook_task': {
            'notebook_path': 'f{load_path_2}'
        },
    }


    notebook_task = DatabricksSubmitRunOperator(
        task_id='notebook_task',
        libraries=pypi_library,
        json=notebook_task_params)
    
    notebook_task = DatabricksRun


    notebook_task2 = DatabricksSubmitRunOperator(
        task_id='notebook_task2',
        libraries=pypi_library,
        json=notebook_task_params2)

    notebook_task >> notebook_task2