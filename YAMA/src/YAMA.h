/*
 * YAMA.h

 *
 *  Created on: 14/9/2017
 *      Author: Dario Poma
 */


#include "../../Biblioteca/src/Biblioteca.c"

#define EVENT_SIZE (sizeof(struct inotify_event)+24)
#define BUF_LEN (1024*EVENT_SIZE)
#define RUTA_NOTIFY "/home/utnso/Escritorio/tp-2017-2c-El-legado-del-Esqui/YAMA";
#define RUTA_CONFIG "/home/utnso/Escritorio/tp-2017-2c-El-legado-del-Esqui/YAMA/YAMAConfig.conf"
#define RUTA_LOG "/home/utnso/Escritorio/tp-2017-2c-El-legado-del-Esqui/YAMA/YAMALog.log"

#define INTSIZE sizeof(int32_t)
#define TEMPSIZE 12
#define nullptr NULL

#define TRANSFORMACION 1 //tambien estan en la biblioteca, pero por alguna razon
#define SOLICITUD 2 //no me los toma
#define REDUCLOCAL 3
#define REDUCGLOBAL 4
#define TERMINADO 5


typedef struct {
	char puertoMaster[50];
	char ipFileSystem[50];
	char puertoFileSystem[50];
	int retardoPlanificacion;
	char algoritmoBalanceo[50];
	int disponibilidadBase;
	bool reconfigurar;
} Configuracion;


typedef struct {
	ListaSockets listaSelect;
	ListaSockets listaMaster;
	Socket maximoSocket;
	Socket listenerMaster;
	Socket fileSystem;
} Servidor;
Servidor* servidor;

typedef struct {
	int32_t nodo;
	int32_t bloque;
	//no pongo los bytes aca porque estan dos veces en bloques gemelos,
	//los manejo aparte
} Bloque;

typedef struct{
	bool conectado;
	uint32_t carga; //son uint32_t porque lo pide el tp, yo usaria ints
	uint32_t tareasRealizadas;
	uint32_t disponibilidad;
	uint32_t nodo; //para comparar con los bloques que reciba
	char ipYPuerto[10]; //para que master sepa quien es quien
} Worker;
Lista workers;

typedef enum {Transformacion,ReduccionLocal,ReduccionGlobal} Etapa;
typedef enum {EnProceso,Finalizado,Error,Abortado} Estado;
int job=-1;
typedef struct{
	int job;
	Socket masterid;
	int32_t nodo;
	int32_t bloque;
	int32_t bytes;
	int32_t nodoAlt;
	int32_t bloqueAlt;
	Etapa etapa;
	char* pathTemporal; //podría usar char[12] y no usar memoria dinamica, despues ver
	Estado estado;
} Entrada;
Lista tablaEstados;
Lista tablaUsados; //entradas Abortadas, Error, o Terminadas que ya
//no se necesitan procesar, solo se dibujan

Configuracion* configuracion; //es un puntero por algun motivo?
ArchivoLog archivoLog;
int estadoYama;

void darPathTemporal(char**,char);
void configurar();
void archivoConfigObtenerCampos();
void pantallaLimpiar();
void yamaIniciar();
void yamaAtender();
void yamaPlanificar(Socket,void*,int);
void actualizarTablaEstados(Entrada*,int);
void dibujarTablaEstados();
void yamaFinalizar();


