# lib de sistema. Indica para o Python o 'caminho' para encontrar os modulos do projeto
import sys
sys.path.append("airflow_pipeline")

from airflow.models import BaseOperator, DAG, TaskInstance
from hook.twitter_hook import TwitterHook
import json
from datetime import datetime, timedelta 
from os.path import join
from pathlib import Path

# classe TwitterOperator herda a classe BaseOperator (classe base para todos os operadores)
class TwitterOperator(BaseOperator):

    # indica em quais campos aceitaremos parametros vindos no estilo 'Jinja Template'
    template_fields = ["file_path","start_time","end_time","query"]

    # quem inicializar o TwitterOperator deve passar os parametros abaixo para o construtor
    def __init__(self, file_path, start_time, end_time, query, **kwargs):
        self.start_time = start_time
        self.end_time = end_time
        self.query = query
        self.file_path = file_path # caminho das pastas/arquivo onde serão armazenados os dados extraidos

        # o construtor da classe BaseOperator espera receber parametros (kwargs)
        super().__init__(**kwargs)

    # cria uma estrutura de pastas, conforme o atributo 'file_path' obtido no construtor
    def create_parent_folder(self):
        # ".parent" indica que todas as pastas superiores devem ser criadas, menos o arquivo em si
        # parents=True diz para a função mkdir criar todas as pastas descritas no caminho
        # exist_ok=True evita que uma pasta já criada seja recriada 
        (Path(self.file_path).parent).mkdir(parents=True, exist_ok=True)

    # execute() é método da classe BaseOperator de implementacao obrigatoria
    # Dentro dele deve constar o código principal responsável pela pesquisa e extração dos dados.
    # A DAG do airflow vai procurar o metodo 'execute()' e enviar o 'context' como parametro.
    def execute(self, context):

        # executa a criacao de pastas p/ armazenamento das info obtidas
        self.create_parent_folder()

        # abre o arquivo no modo escrita 'w' para armazenar os dados extraidos
        with open(self.file_path, "w") as output_file:

            # obtem os tweets. Consulta a api do twitter usando a classe TwitterHook e os parametros recebidos no construtor
            lista_paginada = TwitterHook(self.start_time,self.end_time,self.query).run()

            # percorre todas as páginas de twets contidas na lista principal 
            for page in lista_paginada:
                
                # armazena cada página (json) no arquivo aberto, formatando para que nao aparecam caracteres estranhos.
                json.dump(page, output_file, ensure_ascii=False)

                # realiza a quebra de linha entre paginas
                output_file.write("\n")

# Obs.: Este trecho só é processado qd o arquivo é executado explicitamente (cmd> python3 twitter_operator.py)
#       Caso contrário, se for importado, nao processará este trecho.
if __name__ == "__main__":

    # Dados p/ teste simulando o pedido de um usuário:
    # ------------------------------------------------

    time_zone = datetime.now().astimezone().tzname()  # obtem o time_zone de onde estamos
    TIMESTAMP_FORMAT = f"%Y-%m-%dT%H:%M:%S.00{time_zone}:00"  # formato de data aceito pela API do TWITTER (ISO 8601/RFC 3339)

    # data de inicio da consulta (dia anterior ao atual)
    start_time = (datetime.now() + timedelta(days=-1)).date().strftime(TIMESTAMP_FORMAT)

    # data final da consulta (dia atual)
    end_time = datetime.now().strftime(TIMESTAMP_FORMAT)

    # palavra a ser buscada na consulta à API
    query = "datascience"  


    # DAG >> OPERADORES >> TAREFAS/INSTÂNCIAS
    # Os operadores são nós da DAG. 
    # Cada operador possui suas próprias instâncias (tarefas) que executam scripts, funções ou transferências de dados específicos.
    
    # cria uma DAG
    with DAG(dag_id="TwitterTest", start_date=datetime.now()) as dag:

        # define uma hierarquia de pastas para armazenar os dados extraidos da api do twitter 
        file_path=join("datalake/twitter_datascience",  # define uma pasta global e pasta do projeto especifico
                      f"extract_date={datetime.now().date()}", # cria pastas para cada dia de pesquisa 
                      # nomeia o arquivo final json iniciando pela palavra de pesquisa seguida da data
                      f"datascience_{datetime.now().date().strftime('%Y%m%d')}.json") 

        # Cria um operator
        # instancia o TwitterOperator passando os parametros normais e task_id que relaciona o operator a uma instância/tarefa  
        # file_path indica o local onde todos os dados serão armazenados
        # Obs.: task_id é um dos parametros **kwargs que a classe BaseOperator espera receber 
        to = TwitterOperator(file_path=file_path,start_time=start_time,end_time=end_time,query=query,task_id="test_run") # test_run é um nome que eu defino

        # Cria uma Tarefa/Instância (TaskInstance) passando o TwitterOperator como parametro
        ti = TaskInstance(task=to)

        # executa o metodo execute() do TwitterOperator passando a TaskInstance
        to.execute(ti.task_id)