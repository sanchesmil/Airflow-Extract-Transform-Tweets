# lib de sistema. Indica para o Python o 'caminho' para encontrar os modulos do projeto
import sys
sys.path.append("airflow_pipeline")

from airflow.models import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from os.path import join
from operators.twitter_operator import TwitterOperator
from airflow.utils.dates import days_ago
from pathlib import Path

# Cria uma DAG, iniciando as requisicoes a partir de 6 dias atrás da data atual, com frequencia diaria
with DAG(dag_id="TwitterDAG", start_date=days_ago(6), schedule_interval="@daily") as dag:

     # data do início do intervalo de dados = data_interval_start
    start_time = "{{ data_interval_start.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}"

    # data final do intervalo de dados (data de acionamento do DAG usando) = data_interval_end
    end_time = "{{ data_interval_end.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}"

    # palavra a ser buscada na consulta à API
    query = "datascience"  

    # O Airflow utiliza o Jinja Template {{}}, uma estrutura de modelagem em Python
    # Ele permite passar informações em tempo de execução para as instâncias de tarefa usando variáveis específicas.
    # Exemplo de variáveis: data_interval_start, data_interval_end e ds.

    # 1ª Etapa: DEFINIÇÃO DA ESTRUTURA DE ARMAZENAMENTO DO DATALAKE LOCAL 
    # -------------------------------------------------------------------

    # obtem o caminho completo do projeto
    BASE_PATH = str(Path("~/projetos/projetos_python/proj_twitter_pipeline").expanduser()) # /home/pedro/Documents

    # Define o 'template' da estrutura de pastas do DATALAKE que armazenará os dados extraidos da api do twitter
    BASE_FOLDER = join( BASE_PATH, "datalake/{stage}/twitter_"+query+"/{partition}") # monta o nome do projeto usando parametros com jinja
    
    # seguimenta os dados extraídos por data, criando uma pasta para cada data de extracao. Ex. extract_date=2023-11-06 
    PARTITION = "extract_date={{ data_interval_start.strftime('%Y-%m-%d') }}"
   
    # define o estágio BRONZE para armazenar os dados brutos extraídos 
    STAGE = 'bronze'    

    # ARMAZENAMENTO BRONZE: define o caminho da pasta p/ armazenamento bronze (dados brutos)
    bronze_folder_path = BASE_FOLDER.format(stage=STAGE,partition=PARTITION)

    # nomeia o arquivo final json iniciando pela palavra de pesquisa seguida da data de extracao. ex: datascience_20231106.json
    FILE = query + "_{{ ds_nodash }}.json" # ds_nodash retiar caracteres separadores. 

    # caminho dos arquivos bronze
    bronze_file_path = join(bronze_folder_path, FILE)  

    # 2ª Etapa: EXTRAÇÃO e ARMAZENAMENTO DOS DADOS BRUTOS
    # ---------------------------------------------------
       
    # Efetua a extracao dos dados usando o TwitterOperator
    # file_path indica o local onde todos os dados serão armazenados
    # Obs.: task_id é o nome que aparecerá na interface web do airflow. Ele é um dos parametros **kwargs que a classe BaseOperator espera receber. 
    twitter_extract = TwitterOperator(file_path=bronze_file_path,start_time=start_time,end_time=end_time,query=query,task_id="twitter_extract_"+query) 


    # 3ª Etapa: TRANSFORMACAO DOS DADOS 
    # ---------------------------------

    # obtem a data de processamento da transformacao
    data_processamento = "{{ds}}"

    # define o estágio SILVER para armazenar os dados transformados
    STAGE="silver"
    
    # ARMAZENAMENTO SILVER: define o caminho da pasta p/ armazenamento silver. Ex ../datalake/silver/twitter_datascience
    silver_folder_path = BASE_FOLDER.format(stage=STAGE, partition="") # nao necessita de partition e de file, ou seja, nao seguimentará os dados por data

    # efetua a transformacao dos dados extraídos do twitter utilizando o operator 'SparkSubmitOperator'
    twitter_transform = SparkSubmitOperator(task_id="twitter_transform_datascience", # nome que aparecerá na interface web do airflow
                                            # caminho ate o script/arquivo que executa a transformacao
                                            application=join(BASE_PATH, "src/spark/transformation.py"),
                                            # nome que eu defino
                                            name="twitter_transformation",
                                            # argumentos esperados pelo script que realiza a transformacao
                                            application_args=["--origem",bronze_folder_path,
                                                              "--destino", silver_folder_path, 
                                                              "--process_date", data_processamento]
    )

    # 3ª Etapa: INSIGHT DE DADOS 
    # --------------------------

    STAGE="gold"

    # ARMAZENAMENTO GOLD: define o caminho da pasta p/ armazenamento gold. Ex ../datalake/gold/twitter_datascience
    gold_folder_path = BASE_FOLDER.format(stage=STAGE, partition="")

    # efetua a extracao de dados ouro a partir da camada prata para insights dos analistas de dados
    twitter_insight = SparkSubmitOperator(task_id="twitter_insight_datascience",
                                          application=join(BASE_PATH,"src/spark/insight_tweet.py"),
                                          name="twitter_insight",
                                          application_args=[
                                              "--origem",silver_folder_path,
                                              "--destino",gold_folder_path,
                                              "--data_processamento", data_processamento
                                          ])

# define a ordem de execucao dos operators
twitter_extract >> twitter_transform >> twitter_insight


# Obs.: Para executar este script é necessário configurar a conexao entre o SPARK e o AIRFLOW na aplicacao web do airflow.
