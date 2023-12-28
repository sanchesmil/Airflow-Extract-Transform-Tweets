from airflow.providers.http.hooks.http import HttpHook
from datetime import datetime,timedelta
import requests
import json

# Define uma classe de conexão à API do Twitter herdando a classe HttpHook que lida com requisicoes do tipo http
class TwitterHook(HttpHook):

    # define o construtor
    def __init__(self, start_time, end_time, query, conn_id=None): 

        self.start_time = start_time  # inicio da consulta
        self.end_time = end_time      # fim da consulta
        self.query = query            # palavra a ser buscada  

        # "twitter_default" é o nome da conexao criada na área de HOOK do Web Service do Airflow
        self.conn_id = conn_id or "twitter_default"

        # informa ao HttpHook o nome da conexao que desejo usar. Assim ele saberá se comunicar dentro do Airflow
        super().__init__(http_conn_id=self.conn_id)

    # monta a url base de consulta
    def create_url(self):
        
        # filtra os campos do tweet conforme minha necessidade
        tweet_fields = "tweet.fields=author_id,conversation_id,created_at,id,in_reply_to_user_id,public_metrics,lang,text"

        # filtra campos do user que publicou o tweet
        user_fields = "expansions=author_id&user.fields=id,name,username,created_at"

        # define a url de consulta a API. 
        # obs: lembrando que a 'base_url' (https://labdados.com) foi configurada nas conexões de HOOK do airflow
        url_raw = f"{self.base_url}/2/tweets/search/recent?query={self.query}&{tweet_fields}&{user_fields}&start_time={self.start_time}&end_time={self.end_time}"
        
        return url_raw
    
    # metodo que prepara e realiza a requisição ao endpoint
    def connect_to_endpoint(self, url, session):

        # classe Request é usada para preparar uma requisicao, passando o metodo http e a url
        request = requests.Request('get',url)

        # joga a requisicao na sessao que recebemos por parametro
        prep = session.prepare_request(request) # metodo padrao da lib HttpHook

        # mostra nos logs a url consultada
        self.log.info(f"URL: {url}")

        # realiza a requisicao no endpoint 
        return self.run_and_check(session,prep, {})
    
    # Método que vai iterar utilizando o 'next_token' para criar novas URLs e assim ter acesso a todas as páginas de resposta da API.
    def paginate(self, url_raw, session):
        # define uma lista principal para armazenar todas as paginas retornadas (sublistas de twets)
        lista_json_reponse = [] # lista de sublistas

        # realiza a primeira requisição
        response = self.connect_to_endpoint(url_raw,session)

        # obtem os dados em json
        response_json = response.json()

        # adiciona o resultado json à lista
        lista_json_reponse.append(response_json)

        # define um contador para limitar o numero de páginas retornadas
        contador = 1

        # verifica se o retorno da requisiçao possui o atributo 'next_token' entre os metadados retornados
        # o atributo 'next_token' indica que existem mais páginas de dados a serem retornados
        while "next_token" in response_json.get("meta", {}) and contador < 100:

            # obtem o next_token da response anterior
            next_token = response_json['meta']['next_token']

            # define a nova url com o parametro 'next_token'
            url = f'{url_raw}&next_token={next_token}'

            # realiza uma nova requisicao com o 'next_token'
            response = self.connect_to_endpoint(url, session)

            # obtem os dados em json
            reponse_json = response.json()

            # adiciona o resultado json à lista
            lista_json_reponse.append(reponse_json)

            # incrementa o contador
            contador += 1
            
        return lista_json_reponse
    
    # para executarmos HpptHook devemos declarar o metodo run.
    def run(self):
        
        # Pedimos a sessao ao HttpHook. 
        # Ele retorna um objeto Session para gerenciar e persistir configurações entre solicitações (cookies, autenticação, proxies).
        session = self.get_conn()

        # criamos a url base
        url_raw = self.create_url()

        # retorna uma lista contendo as sublistas de twets (paginas)
        return self.paginate(url_raw,session)
    
# ----------------
# ÁREA DE TESTES
# ----------------

# Obs.: Este trecho só é processado qd o arquivo é executado explicitamente (cmd> python3 twitter_hook.py)
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
    query = "data science"  
    
    # --------------------------------------------------

    # instancia a classe TwitterHook e executa o metodo run() retornando a lista final
    lista_paginada = TwitterHook(start_time,end_time,query).run()

    # percorre todas as páginas de twets contidas na lista principal
    for page in lista_paginada:
        # imprime os twets
        print(json.dumps(page, indent=4, sort_keys=True))