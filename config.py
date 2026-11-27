"""Project paths and the supported local Spark runtime."""
from pathlib import Path
import os
import sys

ROOT = Path(__file__).resolve().parent
DATA = Path(os.environ.get('COMMERCEFLOW_DATA', ROOT / 'data')).resolve()
DEFAULT_SOURCE = ROOT / 'online+retail' / 'Online Retail.xlsx'


def spark_session(name: str = 'CommerceFlow'):
    from pyspark.sql import SparkSession
    from delta import configure_spark_with_delta_pip
    runtimes = sorted((ROOT / '.runtime').glob('jdk-17*'))
    if runtimes:
        os.environ['JAVA_HOME'] = str(runtimes[0])
    import pyspark
    os.environ['SPARK_HOME'] = str(Path(pyspark.__file__).parent)
    hadoop = ROOT / '.runtime' / 'hadoop'
    if hadoop.exists():
        os.environ.setdefault('HADOOP_HOME', str(hadoop))
        os.environ['PATH'] = str(hadoop / 'bin') + os.pathsep + os.environ['PATH']
    os.environ.setdefault('SPARK_LOCAL_IP', '127.0.0.1')
    # Spark's Windows discovery script does not quote PYTHON_RUNNER; use PATH
    # to resolve this exact virtual environment without embedding spaced paths.
    os.environ['PATH'] = str(Path(sys.executable).parent) + os.pathsep + os.environ['PATH']
    python_runner = 'python.exe' if os.name == 'nt' else sys.executable
    os.environ['PYSPARK_PYTHON'] = python_runner
    os.environ['PYSPARK_DRIVER_PYTHON'] = python_runner
    builder = (SparkSession.builder.appName(name).master('local[2]')
               .config('spark.ui.enabled', 'false')
               .config('spark.driver.host', '127.0.0.1')
               .config('spark.driver.bindAddress', '127.0.0.1')
               .config('spark.sql.shuffle.partitions', '2')
               .config('spark.databricks.delta.snapshotPartitions', '2')
               .config('spark.sql.session.timeZone', 'UTC')
               .config('spark.sql.caseSensitive', 'true')
               .config('spark.sql.legacy.timeParserPolicy', 'CORRECTED')
               .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
               .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
               .config('spark.jars.ivy', str(ROOT / '.runtime' / 'ivy')))
    jars = list((ROOT / '.runtime' / 'jars').glob('*.jar'))
    if jars:
        classpath = os.pathsep.join(str(p) for p in jars)
        builder = (builder.config('spark.driver.extraClassPath', classpath)
                   .config('spark.executor.extraClassPath', classpath))
    else:
        builder = configure_spark_with_delta_pip(builder)
    session = builder.getOrCreate()
    session.sparkContext.setLogLevel('WARN')
    return session
