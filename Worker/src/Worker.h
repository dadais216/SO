/*
 * Worker.h

 *
 *  Created on: 18/9/2017
 *      Author: Dario Poma
 */

#include "../../Biblioteca/src/Biblioteca.c"

#define RUTA_CONFIG "../NodoConfig.conf"
#define RUTA_LOG "../../../WorkerLog.log"

typedef struct {
	char ipFileSytem[50];
	char puertoFileSystem[50];
	char nombreNodo[50];
	char puertoWorker[50];
	char rutaDataBin[100];
} Configuracion;

String campos[5];
Configuracion* configuracion;
ArchivoLog archivoLog;
int estadoWorker;
Socket socketListenerWorker;
void socketAceptarConexion();


void workerIniciar();
