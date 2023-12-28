from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from os.path import join
import argparse


# Define uma funcao que realiza uma consulta sobre os dados da camada silver
# Ela realiza a agregaçao de informacoes para cada dia analisado.
# Retorno: 
# - dia (Y-m-d)
# - qtd de pessoas distintas que estão fazendo Tweets relacionados ao tema data science
# - somatorio de curtidas (likes)
# - somatorio de citações (quote)
# - somatorio de respostas (reply)
# - somatorio de retransmições (retweets) 
# - dia da semana
def get_tweet_conversas(df_tweet):

    tweet_conversas = df_tweet.alias("tweet")\
                  .groupBy(f.to_date("created_at").alias("created_date"))\
                  .agg(
                      f.countDistinct("author_id").alias("n_tweets"),
                      f.sum("like_count").alias("n_like"),
                      f.sum("quote_count").alias("n_quote"),
                      f.sum("reply_count").alias("n_reply"),
                      f.sum("retweet_count").alias("n_retweet") )\
                  .withColumn("weekday", f.date_format("created_date", "E"))
    
    return tweet_conversas

# define o destino dos dados obtidos
def export_json(df, destino):

    # Monta a estrutura que será salva em arquivo 
    # - método .coalesce(1): o parâmetro 1 é responsável por unir as informações que o Spark quebra em pedaços
    # - método .write.mode("overwrite"): o parâmetro utilizado serve para evitar problemas ao encontrar os dados já salvos
    # - método .json(destino): indica que salvaremos em formato JSON no caminho indicado no parâmetro
   df.coalesce(1).write.mode("overwrite").json(destino)

# funcao principal que executa as funcoes de agregacao de dados e de criacao de arquivos de dados no destino
def twitter_insight(spark, origem, destino, data_processamento):

    # obtem os dados de tweets da camada silver do datalake 
    df_tweet = spark.read.json(join(origem, 'tweet'))

    # obtem os dados de tweets agregados pela data de criacao do tweet
    tweets_conversas = get_tweet_conversas(df_tweet)

    # constrói o caminho final dos dados (pasta gold) incluindo uma pasta para cada data de processamento
    dest_final = join(destino, f"process_date={data_processamento}")

    # salva os dados agregados no destino final  
    export_json(tweets_conversas, dest_final)


# executa o 'main' qd o arquivo for executado
if __name__ == "__main__" :
    
    # obtem a sessao spark
    spark = SparkSession\
        .builder\
        .appName("twitter_transformation")\
        .getOrCreate()
    
    # define um parser que receberá dados em tempo de execucao 
    # Obs: quando o usuário/chamador executar este arquivo deverá passar os argumentos esperados. (similar ao prompt de entrada de dados)
    parser = argparse.ArgumentParser(
        description="Spark Twitter Insight" # dá um nome para o parser
    )

    # define os argumentos do parser que deverão ser informados pelo usuario. Eles serão usados como parametros de outra funcao 
    parser.add_argument("--origem", required=True)
    parser.add_argument("--destino", required=True)
    parser.add_argument("--data_processamento", required=True)

    # extrai os argumentos preenchidos do parser
    args = parser.parse_args()

    # chama a funcao geral de insights passando os parametros esperados
    twitter_insight(spark, args.origem, args.destino, args.data_processamento)


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
    # Terminal na raiz do projeto> ./spark-3.1.3-bin-hadoop3.2/bin/spark-submit src/spark/transformation.py --origem datalake/twitter_datascience/tweet --destino src/spark --data_processamento <data_processamento>
    



    

