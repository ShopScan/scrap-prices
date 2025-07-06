#!/bin/bash

#=============================================================================
#SETUPAUTOMATIZADODELPROYECTOSCRAPPRICES
#=============================================================================

echo"ConfigurandoproyectoScrapPrices..."
echo""

#Coloresparaoutput
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

#Verificarprerrequisitos
echo-e"${BLUE}Verificandoprerrequisitos...${NC}"

if!command-vdocker&>/dev/null;then
echo-e"${RED}ERROR:Dockernoestáinstalado${NC}"
echo"InstalaDocker:https://docs.docker.com/get-docker/"
exit1
fi

if!command-vdocker-compose&>/dev/null;then
echo-e"${RED}ERROR:DockerComposenoestáinstalado${NC}"
echo"InstalaDockerCompose:https://docs.docker.com/compose/install/"
exit1
fi

echo-e"${GREEN}OK:DockeryDockerComposeestáninstalados${NC}"

#Configurarcredenciales
echo""
echo-e"${BLUE}�Configurandocredenciales...${NC}"

mkdir-pcredentials

if[!-f"credentials/gcp-service-account.json"];then
echo-e"${YELLOW}ArchivodecredencialesGCPnoencontrado${NC}"
echo""
echo"Paraobtenerlascredenciales:"
echo"1.VeaGoogleCloudConsole"
echo"2.IAM&Admin>ServiceAccounts"
echo"3.CreaunServiceAccountconrolBigQueryAdmin"
echo"4.DescargaelarchivoJSON"
echo"5.Guárdalocomo:credentials/gcp-service-account.json"
echo""
read-p"¿Hascolocadoelarchivodecredenciales?(y/N):"-n1-r
echo""
if[[!$REPLY=~^[Yy]$]];then
echo-e"${RED}Setupcancelado${NC}"
echo"Configuralascredencialesyejecuta:./setup.sh"
exit1
fi
fi

if[!-f"credentials/gcp-service-account.json"];then
echo-e"${RED}CredencialesGCPnoencontradasencredentials/gcp-service-account.json${NC}"
exit1
fi

echo-e"${GREEN}CredencialesGCPencontradas${NC}"

#Generarconfiguración
if[!-f".env"];then
echo""
echo-e"${BLUE}�Generandoconfiguraciónsegura...${NC}"
./generate_secrets.sh
else
echo-e"${GREEN}Archivo.envyaexiste${NC}"
fi

#ConfigurarProjectID
echo""
echo-e"${BLUE}ConfiguracióndeGoogleCloudPlatform${NC}"
current_project=$(grep"GCP_PROJECT_ID=".env2>/dev/null|cut-d'='-f2)

if["$current_project"=="tu_project_id_aqui"]||[-z"$current_project"];then
read-p"IngresatuGCPProjectID:"GCP_PROJECT_ID
if[-n"$GCP_PROJECT_ID"];then
sed-i"s/GCP_PROJECT_ID=.*/GCP_PROJECT_ID=$GCP_PROJECT_ID/".env
echo-e"${GREEN}ProjectIDconfigurado:$GCP_PROJECT_ID${NC}"
fi
else
echo-e"${GREEN}ProjectIDyaconfigurado:$current_project${NC}"
fi

#Verificacióndeseguridad
echo""
echo-e"${BLUE}Verificandoconfiguracióndeseguridad...${NC}"
./check_security.sh

if[$?-ne0];then
echo-e"${RED}Problemasdeseguridaddetectados${NC}"
exit1
fi

#Construireiniciarservicios
echo""
echo-e"${BLUE}Construyendocontenedores...${NC}"
docker-composebuild

echo""
echo-e"${BLUE}Iniciandoservicios...${NC}"
docker-composeup-d

#Esperarservicios
echo""
echo-e"${BLUE}Esperandoservicios(30s)...${NC}"
sleep30

#Mostrarestado
echo""
echo-e"${BLUE}Estadodeservicios:${NC}"
docker-composeps

#Obtenercredenciales
ADMIN_PASSWORD=$(grepAIRFLOW_ADMIN_PASSWORD.env|cut-d'='-f2)
MINIO_USER=$(grepMINIO_ROOT_USER.env|cut-d'='-f2)
MINIO_PASSWORD=$(grepMINIO_ROOT_PASSWORD.env|cut-d'='-f2)

echo""
echo-e"${GREEN}¡Setupcompletadoexitosamente!${NC}"
echo""
echo-e"${BLUE}Serviciosdisponibles:${NC}"
echo"•AirflowWebUI:http://localhost:8080"
echo"Usuario:admin"
echo"Password:$ADMIN_PASSWORD"
echo""
echo"•MinIOConsole:http://localhost:9001"
echo"Usuario:$MINIO_USER"
echo"Password:$MINIO_PASSWORD"
echo""
echo-e"${BLUE}Próximospasos:${NC}"
echo"1.AccederaAirflowWebUI"
echo"2.ActivarlosDAGsnecesarios"
echo"3.Monitorearejecuciones"
echo""
echo-e"${BLUE}Comandosútiles:${NC}"
echo"•Verlogs:docker-composelogs-f"
echo"•Reiniciar:docker-composerestart"
echo"•Detener:docker-composedown"
