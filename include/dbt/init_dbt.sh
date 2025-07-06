#!/bin/bash

#Scriptdeinicializaciónparadbt
#Ejecutarestescriptparaconfigurardbtporprimeravez

set-e

echo"🚀Inicializandoproyectodbt..."

#Variables
DBT_PROJECT_PATH="/opt/airflow/include/dbt"
PROFILES_PATH="/opt/airflow/include/dbt"

#Configurarvariablesdeentorno
exportDBT_PROFILES_DIR=$PROFILES_PATH
exportGOOGLE_APPLICATION_CREDENTIALS="/opt/airflow/shop-scan-ar-40e81820454a.json"

echo"📁Directoriodelproyectodbt:$DBT_PROJECT_PATH"
echo"📁Directoriodeprofiles:$PROFILES_PATH"
echo"🔐CredencialesdeGCP:$GOOGLE_APPLICATION_CREDENTIALS"

#Cambiaraldirectoriodelproyecto
cd$DBT_PROJECT_PATH

echo"🔍Verificandoconfiguracióndedbt..."
dbtdebug

echo"📦Instalandodependenciasdedbt..."
dbtdeps

echo"🏗️CreandodatasetsenBigQuerysinoexisten..."
#LosdatasetssecreanmedianteelDAGdeAirflow

echo"✅Inicializacióncompleta!"
echo""
echo"🎯Próximospasos:"
echo"1.EjecutarelDAG'dbt_bigquery_transformation'enAirflow"
echo"2.Verificarquelosmodelosseejecutencorrectamente"
echo"3.Revisarladocumentacióngeneradacon'dbtdocs'"
echo""
echo"📖Comandosútiles:"
echo"dbtrun#Ejecutartodoslosmodelos"
echo"dbttest#Ejecutartests"
echo"dbtdocsgenerate#Generardocumentación"
echo"dbtdocsserve#Servirdocumentaciónenpuerto8080"
