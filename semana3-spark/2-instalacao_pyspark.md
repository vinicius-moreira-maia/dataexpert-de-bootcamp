1. O pyspark já vem na instalação do spark completo, mas é preciso adicioná-lo à variável PYTHONPATH
`export PYTHONPATH="${SPARK_HOME}/python/:$PYTHONPATH"`
`export PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9.5-src.zip:$PYTHONPATH"`

2. Verificar se a versão do py4j do comando anterior está correta
`cd spark/spark-3.3.2-bin-hadoop3/python/lib/`
`ls`

Se a versão do comando do passo 1 for diferente da versão que apareceu no 'ls', adaptar.

3. Testar o pyspark

baixando um csv de testes (pode ser qualquer um)
`wget https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv`

import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()

df = spark.read \
    .option("header", "true") \
    .csv('taxi_zone_lookup.csv')

df.show()

para testar a escrita:
df.write.parquet('zones')

4. obs.: Os imports parecerão estar com erro, mas não estão. Isso é apenas porque o vscode não consegue encontrar o 
caminho do pyspark (pois não foi instalado via pip), mas ele é visível pro Linux pois as variáveis de ambiente foram configuradas.

5. Incluir essas linhas no arquivo .bashrc. Isso evita ter que ficar digitando sempre que o wsl for reiniciado.

pasta raiz: `cd`
abrir o arquivo no vscode: `code .bashrc`

export JAVA_HOME="${HOME}/spark/jdk-11.0.2"
export PATH="${JAVA_HOME}/bin:${PATH}"
export SPARK_HOME="${HOME}/spark/spark-3.3.2-bin-hadoop3"
export PATH="${SPARK_HOME}/bin:${PATH}"
export PYTHONPATH="${SPARK_HOME}/python/:$PYTHONPATH"
export PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9.5-src.zip:$PYTHONPATH"
