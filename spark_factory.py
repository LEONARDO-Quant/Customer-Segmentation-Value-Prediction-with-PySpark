import os, sys
from pyspark.sql import SparkSession

def get_spark(app_name="WinLocalhostOnly"):
# 1. Alignement de l'exécutable Python (évite les conflits d'environnements)
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    
    # 2. Fix notebook Windows / Py4J
    os.environ["PYSPARK_PIN_THREAD"] = "true"
    
    # 3. Construction de la Session Spark
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("WinLocalhostOnly")
        .config("spark.ui.enabled", "false")
        # Force localhost partout
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.local.ip", "127.0.0.1")
        # Force IPv4 JVM
        .config("spark.driver.extraJavaOptions", "-Djava.net.preferIPv4Stack=true")
        .config("spark.executor.extraJavaOptions", "-Djava.net.preferIPv4Stack=true")
        # Evite certains soucis worker Windows
        .config("spark.python.use.daemon", "true")
        .config("spark.python.worker.reuse", "true")
        # Alloue 4 Go (ou plus selon ta RAM)
        .config("spark.driver.memory", "4g") 
        .getOrCreate()
    )

    return spark