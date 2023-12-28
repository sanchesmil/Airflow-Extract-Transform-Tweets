from datetime import datetime, timedelta
from os.path import join
import requests
import json  # lib usada p/ manipular objetos JSON
import os # lib usada p/ acessar o Sist. Operacional

#--------------------------------------------------------
#-- CRIA AS VARIÁVEIS USADAS NA CONSULTA A API DO TWITTER
#--------------------------------------------------------

# obtem o time_zone de onde estamos
time_zone = datetime.now().astimezone().tzname()

# formato de data aceito pela API do TWITTER (ISO 8601/RFC 3339)
TIMESTAMP_FORMAT = f"%Y-%m-%dT%H:%M:%S.00{time_zone}:00"  

# data de inicio da consulta (dia anterior ao atual)
start_time = (datetime.now() + timedelta(days=-1)).date().strftime(TIMESTAMP_FORMAT)

# data final da consulta (dia atual)
end_time = datetime.now().strftime(TIMESTAMP_FORMAT)

# palavra a ser buscada na consulta à API
query = "data science"  

# filtra os campos do tweet conforme minha necessidade
tweet_fields = "tweet.fields=author_id,conversation_id,created_at,id,in_reply_to_user_id,public_metrics,lang,text"


# filtra campos do user que publicou o tweet
user_fields = "expansions=author_id&user.fields=id,name,username,created_at"

#---------------------------------------------
#-- MONTA A URL DE CONSULTA NA API DO TWITTER
#---------------------------------------------

# define a url base de consulta a API. (a função join() é usada para concatenar strings)
url_raw = join("https://labdados.com/2/tweets/search/recent?query=",
          f"{query}&{tweet_fields}&{user_fields}&start_time={start_time}&end_time={end_time}")

#-------------------------------------------------------------------------
#-- OBTEM o TOKEN DE AUTORIZAÇÃO gravado em uma VARIÁVEL DE AMBIENTE no SO e MONTA O HEADER
#-------------------------------------------------------------------------

# usa a lib 'os' (sistema operacional) para pegar uma var de ambiente 
# Obs.: Esta var deve ter sido definida previamente. Ex: cmd> export BEARER_TOKEN="<token_autorizacao_api>"
bearer_token = os.environ.get("BEARER_TOKEN")

headers =  {"Authorization": "Bearer {}".format(bearer_token)}

#---------------------------------------------
#-- DEFINE CONFIG PROXY (SE NECESSÁRIO)
#---------------------------------------------
proxies = {
    'http': 'http://10.228.108.100:8080',
    'https': 'http://10.228.108.100:8080'
}

#--------------------------------------
#-- REALIZA A CONSULTA À API DO TWITTER DE FORMA PAGINADA, conforme o atributo NEXT_TOKEN
#--------------------------------------

# realiza a primeira requisição
response_json = requests.get(url_raw,proxies=proxies).json()

# imprime a primeira página retornada
print(json.dumps(response_json, indent=4, sort_keys=True))

# verifica se o retorno da requisiçao possui o atributo 'next_token' entre os metadados retornados
# o atributo 'next_token' indica que existem mais páginas de dados a serem retornados
# paginate
while "next_token" in response_json.get("meta", {}):

    # obtem o next_token da response anterior
    next_token = response_json['meta']['next_token']

    # define a url final para obter os próximos tweets de forma paginada
    url = f'{url_raw}&next_token={next_token}'

    # realiza a requisicao 
    response_json = requests.get(url,proxies=proxies).json()

    print(json.dumps(response_json, indent=4, sort_keys=True))

