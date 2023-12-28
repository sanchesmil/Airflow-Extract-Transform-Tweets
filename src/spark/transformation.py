from pyspark.sql import functions as f
from pyspark.sql import SparkSession
from os.path import join
import argparse

# extrai os dados de tweets
# recebe um dataframe bruto e extrai os tweets formatados
def get_tweets_data(df):
    tweet_df = df.select(f.explode("data").alias("tweets"))\
                 .select("tweets.author_id", "tweets.conversation_id", \
                        "tweets.created_at", "tweets.id", \
                        "tweets.public_metrics.*", "tweets.text")
    return tweet_df

# extrai os dados de usarios
# recebe um dataframe bruto e retorna os usuarios formatados
def get_users_data(df):

    users_df = df.select(f.explode("includes.users").alias("users")).select("users.*")
    
    return users_df

# exporta o dataframe para o caminho indicado
def export_json(df, path):

    # Monta a estrutura que será salva em arquivo 
    # - método .coalesce(1): o parâmetro 1 é responsável por unir as informações que o Spark quebra em pedaços
    # - método .write.mode("overwrite"): o parâmetro utilizado serve para evitar problemas ao encontrar os dados já salvos
    # - método .json('output/tweet'): indica que salvaremos em formato JSON no caminho indicado no parâmetro
   df.coalesce(1).write.mode("overwrite").json(path)

# executa as transformacoes anteriores 
# Parans: sessao do spark, dados brutos, endereco destino, data de processamento
# Obs.: O Airflow é quem será responsavel por criar uma sessao do spark
def twitter_transformation(sparkSession, origem, destino, process_date):

    # obtem os dados brutos em um dataframe do spark
    df = sparkSession.read.json(origem)    
    
    # faz as extracoes com dados transformados
    tweets = get_tweets_data(df)
    users = get_users_data(df)

    # constrói o caminho final dos dados
    # {type_name} é uma variável que representa o tipo de dado que queremos salvar 
    dest_final = join(destino, "{type_name}", f"process_date={process_date}")

    export_json(tweets, dest_final.format(type_name="tweet"))  # type_name = tweet
    export_json(users, dest_final.format(type_name="user"))    # type_name = user


# script de execucao de teste
if __name__ == "__main__":

    # obtem a sessao spark
    spark = SparkSession\
        .builder\
        .appName("twitter_transformation")\
        .getOrCreate()

    # define um parser que receberá dados em tempo de execucao 
    # Obs: quando o usuário/chamador executar este arquivo deverá passar os argumentos esperados. (similar ao prompt de entrada de dados)
    parser = argparse.ArgumentParser(
        description="Spark Twitter Transformation" # dá um nome para o parser
    )

    # define os argumentos do parser que deverão ser informados pelo usuario. Eles serão usados como parametros de outra funcao 
    parser.add_argument("--origem", required=True)
    parser.add_argument("--destino", required=True)
    parser.add_argument("--process_date", required=True)

    # extrai os argumentos preenchidos do parser
    args = parser.parse_args()

    # chama a funcao geral de transformacao passando os parametros/argumentos
    twitter_transformation(spark, args.origem, args.destino, args.process_date)

    
    # Executando este arquivo/script
    # 1 - Para executar este arquivo é necessário ter o SPARK instalado na máquina ou no ambiente venv
    # 2 - No terminal, informar o caminho para chegar no comando 'spark-submit' + caminho do arquivo. Ex.: cmd> /opt/spark/bin/spark-submit <caminho do arquivo> 
    # 3 - Passar os argumentos esperados. Ex. cmd> ..... --origem <origem-arquivos> --destino <destino-arquivos> -- process_date <data do sistema>
    
    # Exemplo: 
    # path_arquivo = "/home/projetos/projetos_python/proj_dados_twitter/src/spark/transformation.py"
    # path_origem = "/home/projetos/projetos_python/proj_dados_twitter/datalake/twitter_datascience"
    # path_destino = "/home/projetos/projetos_python/proj_dados_twitter/src/spark"
    # data_processamento = "2022-11-10"
    #
    # Com o SPARK instalado no sistema operacional
    # cmd> sudo /opt/spark/bin/spark-submit <path_arquivo> --origem <path_origem> --destino <path_destino> --process_data <data_processamento>
    # ou 
    # Com o SPARK instalado no próprio ambiente VENV
    # Terminal na raiz do projeto> ./spark-3.1.3-bin-hadoop3.2/bin/spark-submit src/spark/transformation.py --origem datalake/twitter_datascience --destino src/spark --process_data <data_processamento>
    