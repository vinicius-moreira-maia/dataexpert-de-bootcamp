Instalando o Spark 3.3.2 (Linux)

1. Criar a pasta "spark" na pasta raiz do usuário
`mkdir spark`
`cd spark`

2. Baixar e descompactar o java na pasra spark
`wget https://download.java.net/java/GA/jdk11/9/GPL/openjdk-11.0.2_linux-x64_bin.tar.gz`
`tar xzfv openjdk-11.0.2_linux-x64_bin.tar.gz`

3. Definir o JAVA_HOME e adicioná-lo ao PATH
`export JAVA_HOME="${HOME}/spark/jdk-11.0.2"`
`export PATH="${JAVA_HOME}/bin:${PATH}"`

verificando: `java --version`

remover o arquivo compactado: `rm openjdk-11.0.2_linux-x64_bin.tar.gz`

4. Baixar e descompactar o Spark
`wget https://archive.apache.org/dist/spark/spark-3.3.2/spark-3.3.2-bin-hadoop3.tgz`
`tar xzfv spark-3.3.2-bin-hadoop3.tgz`

remover o arquivo compactado: `rm spark-3.3.2-bin-hadoop3.tgz`

5. Definir SPARK_HOME e adicioná-lo ao PATH
`export SPARK_HOME="${HOME}/spark/spark-3.3.2-bin-hadoop3"`
`export PATH="${SPARK_HOME}/bin:${PATH}"`

6. Testar o spark
`spark-shell`

executar:
val data = 1 to 10000
val distData = sc.parallelize(data)
distData.filter(_ < 10).collect()

sair:
:quit

