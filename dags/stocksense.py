import airflow.utils.dates
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


from urllib import request

dag = DAG(
    dag_id="stocksense_bashoperator",
    start_date=airflow.utils.dates.days_ago(3),
    schedule_interval="@daily",
    catchup=False

)


def _get_data(output_path, **context):
    print("before","----------------------------------",context["execution_date"])

    year, month, day, hour, *_ = context["execution_date"].timetuple()
    hour -=1
    print("after","----------------------------------",context["execution_date"].timetuple())
    url = ("https://dumps.wikimedia.org/other/pageviews/"
           f"{year}/{year}-{month:0>2}/pageviews-{year}{month:0>2}{day:0>2}-{hour:0>2}0000.gz"
           )
    print("url-----------------------,",url)
    request.urlretrieve(url, output_path)


get_data = PythonOperator(
    task_id="get_data",
    python_callable=_get_data,
    op_kwargs={"output_path":"/tmp/wikipageviews.gz"},
        dag=dag,
)

extract_data =  BashOperator(
    task_id="unzip_data",
    bash_command="gunzip --force /tmp/wikipageviews.gz",
    dag=dag,
)


def _fetch_pageviews(pagenames):
    result = dict.fromkeys(pagenames, 0)
    with open(f"/tmp/wikipageviews", "r") as f:
        for line in f:
            domain_code, page_title, view_counts, _ = line.split(" ")
            if domain_code == "en" and page_title in pagenames:
                result[page_title] = view_counts
                print(result)

fetch_pageviews = PythonOperator(
task_id="fetch_pageviews",
    python_callable=_fetch_pageviews,
    op_kwargs={
            "pagenames": {
            "Google",
            "Amazon",
            "Apple",
            "Microsoft",
            "Facebook",
            }
    },
    dag=dag,
)


get_data >> extract_data>>fetch_pageviews