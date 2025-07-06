#[SCRAPER]ScraperdePrecios-SupermercadosArgentinos

SistemaautomatizadodescrapingdepreciosusandoApacheAirflow,DockeryGoogleCloudPlatform.

##[ARQUITECTURA]Arquitectura

-**ApacheAirflow**:Orquestacióndetareas
-**Playwright**:Webscrapingresistente
-**PostgreSQL**:BasededatosdeAirflow
-**MinIO**:Almacenamientodeobjetos
-**BigQuery**:Datawarehouse
-**dbt**:Transformacióndedatos
-**Docker**:Containerización

##[SETUP]InstalaciónRápida

###Prerrequisitos
-DockeryDockerCompose
-CuentadeGoogleCloudPlatform
-ServiceAccountdeGCPconpermisosBigQuery

###SetupAutomático

```bash
#1.Clonarrepositorio
gitclonehttps://github.com/tu-usuario/scrap-prices.git
cdscrap-prices

#2.ConfigurarcredencialesGCP
mkdir-pcredentials
#CopiatuarchivoJSONdeGCPaquí:credentials/gcp-service-account.json

#3.Setupcompletoautomático
./setup.sh
```

Elscript`setup.sh`automáticamente:
-✅Verificaprerrequisitos
-🔐Generacredencialesseguras
-[SERVICIOS]ConfiguraProjectIDdeGCP
-🐳Construyeeiniciaservicios
-[USO]Muestracredencialesdeacceso

##[SERVICIOS]AccesoaServicios

Despuésdelsetup:

-**AirflowWebUI**:http://localhost:8080
-**MinIOConsole**:http://localhost:9001

Lascredencialessemuestranalfinaldelsetup.

##[USO]Uso

1.**ActivarDAGs**enAirflowWebUI:
-`dulce_de_leche_unified`:Scrapingunificado
-`carrefour_dulce_de_leche_prices`:Carrefourespecífico
-`vea_sucursales`:Informaciónsucursales
-`dbt_bigquery_transformation`:Transformaciones

2.**Monitorear**ejecucionesenlainterfaz
3.**Datos**almacenadosautomáticamenteenBigQuery

##[CONFIG]Configuración

###AgregarProductos

Editaarchivosen`dags/src/`:
-`carrefour_product_configs.py`
-`jumbo_product_configs.py`
-`vea_product_configs.py`

###EstructuradeProducto
```python
{
'name':'NombredelProducto',
'search_term':'términobúsqueda',
'brand_filter':'marca(opcional)',
'price_selector':'selectorCSSprecio',
'title_selector':'selectorCSStítulo'
}
```

##🗂️EstructuradelProyecto

```
scrap-prices/
├──airflow/#Dockerconfig
├──dags/#DAGsAirflow
│├──src/#Códigocompartido
│├──dulce_de_leche/#Scrapersespecíficos
│└──utilidades/#Utils
├──include/dbt/#Transformacionesdbt
├──credentials/#Credenciales(NOsubir)
├──.env.example#Templatevariables
└──requirements.txt#Dependencias
```

##[SEGURIDAD]Seguridad

###VerificarAntesdeCommit
```bash
./check_security.sh
```

###ArchivosProtegidos
-`.env`-Variablesdeentorno
-`credentials/`-ArchivosGCP
-`logs/`-Logscondatossensibles

##[COMANDOS]ComandosÚtiles

```bash
#Verlogs
docker-composelogs-f

#Reiniciarservicios
docker-composerestart

#Detenertodo
docker-composedown

#Debugging
docker-composeexecairflow-webserverbash
```

##[CONTRIB]Contribuir

1.Forkelproyecto
2.Creabranch(`gitcheckout-bfeature/nueva-feature`)
3.Commitcambios(`gitcommit-am'Addfeature'`)
4.Pushbranch(`gitpushoriginfeature/nueva-feature`)
5.CreaPullRequest

##[LICENCIA]Licencia

MITLicense-ver`LICENSE`

---

**AVISO:Aviso**:Proyectoeducativo.Respetatérminosdeserviciodesitiosweb.
