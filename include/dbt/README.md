#dbtBigQuerySetup

EstedirectoriocontieneelproyectodbtconfiguradoparatrabajarconBigQuery.

##EstructuradelProyecto

```
include/dbt/
├──dbt_project.yml#Configuraciónprincipaldelproyectodbt
├──profiles.yml#ConfiguracióndeconexiónaBigQuery
├──models/
│├──staging/#Modelosdestaging(views)
││├──stg_raw_prices.sql
││└──_sources.yml
│├──marts/#Modelosfinales(tables)
││├──mart_price_analytics.sql
││└──mart_store_comparison.sql
│└──schema.yml#Documentaciónytestsdemodelos
├──macros/
│└──utils.sql#Macrosreutilizables
└──tests/#Testspersonalizados
```

##Configuración

###1.CredencialesdeBigQuery
-Lascredencialesseencuentranen`/opt/airflow/shop-scan-ar-40e81820454a.json`
-ElproyectodeGCPconfiguradoes:`shop-scan-ar`
-Datasets:
-Desarrollo:`scrap_prices_dev`
-Producción:`scrap_prices_prod`

###2.Modelosincluidos

####Staging(`models/staging/`)
-**stg_raw_prices**:Limpiayestandarizalosdatosrawdescraping

####Marts(`models/marts/`)
-**mart_price_analytics**:Análisisdetendenciasdepreciosconcambiosdiariosysemanales
-**mart_store_comparison**:Comparacióndepreciosentretiendasparacadaproducto

###3.Macros(`macros/`)
-**get_current_timestamp()**:Obtienetimestampactual
-**clean_product_name()**:Limpianombresdeproductos
-**calculate_price_change()**:Calculacambiosporcentualesdeprecios

##ComandosdbtÚtiles

```bash
#Verificarconfiguración
dbtdebug

#Instalardependencias
dbtdeps

#Ejecutartodoslosmodelos
dbtrun

#Ejecutarsolostaging
dbtrun--selectstaging

#Ejecutarsolomarts
dbtrun--selectmarts

#Ejecutartests
dbttest

#Generardocumentación
dbtdocsgenerate

#Servirdocumentación
dbtdocsserve
```

##VariablesdeEntornoNecesarias

```bash
exportDBT_PROFILES_DIR=/opt/airflow/include/dbt
exportGOOGLE_APPLICATION_CREDENTIALS=/opt/airflow/shop-scan-ar-40e81820454a.json
```

##DatosdeEjemplo

ElDAGcreaautomáticamentedatosdeejemploenBigQueryparatesting:
-Productos:CarnedeRes,PolloEntero
-Tiendas:Carrefour,Coto
-Categoría:Carnes

##FlujodeTransformación

1.**RawData**→Tabla`scraped_prices`enBigQuery
2.**Staging**→`stg_raw_prices`(limpiezayestandarización)
3.**Analytics**→`mart_price_analytics`(tendenciasdeprecios)
4.**Comparison**→`mart_store_comparison`(comparaciónentretiendas)

##MonitoreoyTests

-Testsdecalidaddedatosencadamodelo
-Validacióndenot_nullencamposcríticos
-Documentaciónautomáticadetodoslosmodelos
