/*
 * FileSystem.h

 *
 *  Created on: 14/8/2017
 *      Author: Dario Poma
 */

#include "../../Biblioteca/src/Biblioteca.c"

//--------------------------------------- Constantes -------------------------------------

#define RUTA_CONFIG "/home/utnso/Escritorio/tp-2017-2c-El-legado-del-Esqui/FileSystem/FileSystemConfig.conf"
#define RUTA_LOG "/home/utnso/Escritorio/tp-2017-2c-El-legado-del-Esqui/FileSystem/FileSystemLog.log"

#define ID_FORMAT 1
#define ID_RM 2
#define ID_RENAME 3
#define ID_MV 4
#define ID_CAT 5
#define ID_MKDIR 6
#define ID_CPFROM 7
#define ID_CPTO 8
#define ID_CPBLOCK 9
#define ID_MD5 10
#define ID_LS 11
#define ID_INFO 12
#define ID_NODES 13
#define ID_HELP 14
#define ID_EXIT 15
#define FORMAT "format"
#define RM "rm"
#define RMB "rm -b"
#define RMD "rm -d"
#define RENAME "rename"
#define MV "mv"
#define CAT "cat"
#define MKDIR "mkdir"
#define CPFROM "cpfrom"
#define CPTO "cpto"
#define CPBLOCK "cpblock"
#define MD5 "md5"
#define LS "ls"
#define INFO "info"
#define NODES "nodes"
#define HELP "help"
#define EXIT "exit"
#define FLAG_C "--clean"
#define FLAG_B "-b"
#define FLAG_D "-d"
#define FLAG_T "-t"
#define OCUPADO '1'
#define VACIO ""
#define FIN '\0'
#define ENTER '\n'
#define ESPACIO ' '
#define TAB '\t'
#define BARRA '/'
#define RAIZ "/"
#define ID_RAIZ 0
#define ARCHIVO_BINARIO "BINARIO"
#define ARCHIVO_TEXTO "TEXTO"
#define PREFIJO "yamafs:"
#define MAX_STRING 300
#define MAX_NOMBRE 255
#define MAX_DIR 100
#define MAX_IP 20
#define MAX_PUERTO 20
#define MAX_NODO 10
#define MAX_COPIAS 2
#define MAX_TIPOS 10
#define MAX_ARGS 5
#define MAX_CAMPOS 4
#define MAX_PREFIJO 7
#define NUEVO 0
#define NORMAL 1
#define INESTABLE 0
#define ESTABLE 1

#define FRACASO -800
#define EXITO 1
#define DESCONEXION 0
#define ABORTAR 301
#define ALMACENADO 306
#define CIERRE 307

#define ACEPTAR_DATANODE 100
#define LEER_BLOQUE 101
#define ESCRIBIR_BLOQUE 102
#define COPIAR_BLOQUE 103
#define COPIAR_BINARIO 104
#define COPIAR_TEXTO 105
#define FINALIZAR_NODO 106

#define ACEPTAR_YAMA 200
#define ENVIAR_BLOQUES 201
#define ERROR_ARCHIVO 202

//--------------------------------------- Estructuras -------------------------------------

typedef struct {
	int identificador;
	String argumentos[MAX_ARGS];
} Comando;

typedef struct {
	ListaSockets listaSelect;
	ListaSockets listaMaster;
	ListaSockets listaWorkers;
	ListaSockets listaDataNodes;
	Socket contadorSocket;
	Socket listenerYama;
	Socket listenerDataNode;
	Socket listenerWorker;
	Socket yama;
} Servidor;

typedef struct {
	char puertoYama[MAX_PUERTO];
	char puertoDataNode[MAX_PUERTO];
	char puertoWorker[MAX_PUERTO];
	char rutaMetadata[MAX_NOMBRE];
} Configuracion;

typedef struct {
	int identificador;
	int identificadorPadre;
	char nombre[MAX_NOMBRE];
} Directorio;

typedef struct {
	int identificadorPadre;
	int identificadorDirectorio;
	int indiceNombresDirectorios;
	String nombreDirectorio;
	String* nombresDirectorios;
} ControlDirectorio;

typedef struct {
	int identificadorPadre;
	char tipo[MAX_TIPOS];
	char nombre[MAX_NOMBRE];
	Lista listaBloques;
} Archivo;

typedef struct {
	int bytesUtilizados;
	int numeroBloque;
	Lista listaCopias;
} Bloque;

typedef struct {
	char nombreNodo[MAX_NODO];
	int  bloqueNodo;
} Copia;

typedef struct {
	char puerto[MAX_PUERTO];
	char ip[MAX_IP];
	char nombre[MAX_NODO];
	int estado;
	int actividadesRealizadas;
	int bloquesLibres;
	int bloquesTotales;
	Bitmap* bitmap;
	Socket socket;
} Nodo;

typedef struct __attribute__((packed)) {
	Entero numeroBloque;
	char bloque[BLOQUE];
} BloqueNodo;

typedef struct __attribute__((packed)) {
	char ip[MAX_IP];
	char puerto[MAX_PUERTO];
} Direccion;

typedef struct __attribute__((packed)) {
	Dir direccionCopia1;
	Entero numeroBloqueCopia1;
	Dir direccionCopia2;
	Entero numeroBloqueCopia2;
	Entero bytesUtilizados;
} BloqueYama;

//--------------------------------------- Globales -------------------------------------

String campos[MAX_CAMPOS];
Configuracion* configuracion;
ArchivoLog archivoLog;
int estadoControl;
int estadoEjecucion;
int estadoFileSystem;
int flagMensaje;
int directoriosDisponibles;
int flagSocket;
Hilo hiloConsola;
Lista listaDirectorios;
Lista listaArchivos;
Lista listaNodos;
Lista listaSockets;
Bitmap* bitmapDirectorios;
Socket socketYama;
Socket socketDataNode;
Socket socketWorker;
String rutaDirectorioArchivos;
String rutaDirectorioBitmaps;
String rutaDirectorios;
String rutaArchivos;
String rutaNodos;
String rutaBuffer;
Nodo* nodoBuffer;
Bloque* bloqueBuffer;
Archivo* archivoBuffer;
Mutex* mutexTarea;
Mutex* mutexRuta;
Mutex* mutexNodo;
Mutex* mutexBloque;
Mutex* mutexArchivo;
Mutex* mutexListaNodos;
Mutex* mutexListaArchivos;
Mutex* mutexListaDirectorios;
Mutex* mutexListaSockets;
Mutex* mutexBitmapDirectorios;
Mutex* mutexEstadoFileSystem;
Mutex* mutexEstadoEjecucion;
Mutex* mutexEstadoControl;
Semaforo* semaforoTarea;
Semaforo* semaforoFinal;

//--------------------------------------- Funciones de File System -------------------------------------

void fileSystemIniciar();
void fileSystemCrearConsola();
void fileSystemAtenderProcesos();
void fileSystemFinalizar();
void fileSystemReiniciar();
void fileSystemRecuperar();

//--------------------------------------- Funciones de Configuracion -------------------------------------

void configuracionImprimir(Configuracion* configuracion);
Configuracion* configuracionLeerArchivo(ArchivoConfig archivoConfig);
void configuracionIniciarRutas();
void configuracionRutasDestruir();
void configuracionIniciarLog();
void configuracionIniciar();

//--------------------------------------- Funciones de Servidor -------------------------------------

void servidorIniciar(Servidor* servidor);
void servidorFinalizar(Servidor* servidor);
bool servidorCantidadSockets(Servidor* servidor);
void servidorIniciarListaSelect(Servidor* servidor);
void servidorControlarContadorSocket(Servidor* servidor, Socket unSocket);
void servidorEsperarSolicitud(Servidor* servidor);
void servidorFinalizarConexion(Servidor* servidor, Socket unSocket);
void servidorSolicitudMensaje(Servidor* servidor, Socket unSocket);
void servidorIniciarContadorSocket(Servidor* servidor);
void servidorActivarListenerYama(Servidor* servidor);
void servidorActivarListenerDataNode(Servidor* servidor);
void servidorActivarListenerWorker(Servidor* servidor);
void servidorActivarListeners(Servidor* servidor);
void servidorAtenderSolicitudes(Servidor* servidor);
void servidorAceptarConexion(Servidor* servidor, Socket unSocket);
void servidorLimpiarListas(Servidor* servidor);
int servidorSolicitudProceso(Servidor* servidor, Socket unSocket);
int servidorAtenderSolicitud(Servidor* servidor, Socket unSocket);
int servidorAtenderSocket(Servidor* servidor, Socket unSocket);

int servidorSolicitudDataNode(Servidor* servidor, Socket unSocket);
void servidorAtenderDataNode(Servidor* servidor, Socket nuevoSocket);
void servidorFinalizarDataNode(Servidor* servidor, Socket unSocket);
void servidorMensajeDataNode(Servidor* servidor, Mensaje* mensaje, Socket unSocket);
void servidorReconectarDataNode(Servidor* servidor, Nodo* nodoTemporal);
bool nodoNuevo(Nodo* nuevoNodo);
void servidorControlarDataNode(Servidor* servidor, Nodo* nodoTemporal);
void servidorRechazarDataNode(Nodo* nuevoNodo);
void servidorRegistrarDataNode(Servidor* servidor, Nodo* nuevoNodo);
void servidorAdmitirDataNode(Servidor* servidor, Nodo* nuevoNodo);
void servidorAceptarReconexionDataNode(Servidor* servidor, Nodo* nuevoNodo);
void servidorAceptarDataNode(Servidor* servidor, Nodo* nodoTemporal);
void servidorDesactivarDataNode(Servidor* servidor, Nodo* nodo);
void servidorDestruirDataNode(Servidor* servidor, Nodo* nodo);

void servidorSolicitudYama(Servidor* servidor, Socket unSocket);
void servidorFinalizarYama();
void servidorMensajeYama();

void servidorSolicitudWorker(Servidor* servidor, Socket unSocket);
void servidorFinalizarWorker(Servidor* servidor, Socket unSocket);
void servidorMensajeWorker();

//--------------------------------------- Funciones de Socket-------------------------------------

bool socketEsListenerYama(Servidor* servidor, Socket unSocket);
bool socketEsListener(Servidor* servidor, Socket unSocket);
bool socketRealizoSolicitud(Servidor* servidor, Socket unSocket);
bool socketEsDataNode(Servidor* servidor, Socket unSocket);
bool socketEsYama(Servidor* servidor, Socket unSocket);
bool socketEsListenerDataNode(Servidor* servidor, Socket unSocket);
bool socketEsWorker(Servidor* servidor, Socket unSocket);
bool socketEsListenerWorker(Servidor* servidor, Socket unSocket);
void socketListaAgregar(Socket* unSocket);
int socketListaCantidad();
void socketListaEliminar(int posicion);
void socketListaDestruir();
Socket* socketListaObtener(int posicion);
void socketListaLimpiar();

//--------------------------------------- Funciones de Consola -------------------------------------

bool consolaEntradaRespetaLimiteEspacios(String entrada);
bool consolaEntradaSinEspacioEnExtremos(String entrada);
bool consolaEntradaEspaciosSeparados(String entrada);
bool consolaEntradaEspaciosBienUtilizados(String entrada);
bool consolaEntradaSinTabs(String entrada);
bool consolaEntradaTieneEspaciosNecesarios(String entrada);
bool consolaEntradaLlena(String entrada);
bool consolaEntradaDecente(String entrada);
bool consolaComandoTipoUno(String comando);
bool consolaComandoTipoDos(String comando);
bool consolaComandoTipoTres(String comando);
bool consolaComandoExiste(String comando);
bool consolaValidarComandoSinTipo(String* subcadenas);
bool consolaValidarComandoTipoUno(String* subcadenas);
bool consolaValidarComandoTipoDos(String* subcadenas);
bool consolaValidarComandoTipoTres(String* subcadenas);
bool consolaComandoEliminarFlagB(String* subcadenas);
bool consolaComandoEliminarFlagD(String* subcadenas);
bool consolaValidarComandoFlagB(String* subcadenas);
bool consolaValidarComandoFlagD(String* subcadenas);
bool consolaComandoControlarArgumentos(String* subcadenas);
bool consolaValidarComando(String* buffer);
bool consolaflagInvalido(String flag);
void consolaIniciarArgumentos(String* argumentos);
void consolaLiberarArgumentos(String* argumentos);
void consolaObtenerArgumentos(String* buffer, String entrada);
void consolaSetearArgumentos(String* argumentos, String* buffer);
void consolaConfigurarComando(Comando* comando, String entrada);
void consolaEjecutarComando(Comando* comando);
void consolaDestruirComando(Comando* comando, String entrada);
void consolaAtenderComandos();
int consolaIdentificarComando(String comando);
String consolaLeerEntrada();
bool consolaArgumentoEsNumero(String ruta);

//--------------------------------------- Funciones de Comando -------------------------------------

void comandoFormatearFileSystem();
void comandoEliminar(Comando* comando);
void comandoEliminarCopia(Comando* comando);
void comandoEliminarDirectorio(Comando* comando);
void comandoEliminarArchivo(Comando* comando);
void comandoRenombrar(Comando* comando);
void comandoMover(Comando* comando);
void comandoMostrarArchivo(Comando* comando);
void comandoCrearDirectorio(Comando* comando);
void comandoCopiarArchivoAYamaFS(Comando* comando);
int comandoCopiarArchivoDeYamaFS(Comando* comando);
void comandoCopiarBloque(Comando* comando);
void comandoObtenerMD5DeArchivo(Comando* comando);
void comandoListarDirectorio(Comando* comando);
void comandoInformacionArchivo(Comando* comando);
void comandoInformacionNodos();
void comandoAyuda();
void comandoFinalizar();
void comandoError();

//--------------------------------------- Funciones de Directorio -------------------------------------

Directorio* directorioCrear(int indice, String nombre, int padre);
void directorioControlSetearNombre(ControlDirectorio* control);
void directorioBuscarIdentificador(ControlDirectorio* control);
ControlDirectorio* directorioControlCrear(String rutaDirectorio);
bool directorioExisteIdentificador(int identificador);
void directorioIniciarEstructura();
Directorio* directorioBuscar(String path);
void directorioMostrarArchivos(int identificadorPadre);
bool directorioIndiceRespetaLimite(int indice);
bool directorioIndiceEstaOcupado(int indice);
bool directorioExisteIdentificador(int identificador);
bool directorioIndicesIguales(int unIndice, int otroIndice);
bool directorioSonIguales(Directorio* directorio, String nombreDirectorio, int idPadre);
Directorio* directorioCrear(int indice, String nombre, int padre);
int directorioBuscarIdentificadorLibre();
String directorioConfigurarEntradaArchivo(String indice, String nombre, String padre);
void directorioPersistir();
void directorioBuscarIdentificador(ControlDirectorio* control);
int directorioIndicesACrear(String* nombresDirectorios, int indiceDirectorios);
bool directorioHaySuficientesIndices(ControlDirectorio* control);
void directorioControlSetearNombre(ControlDirectorio* control);
ControlDirectorio* directorioControlCrear(String rutaDirectorio);
void directorioControlarEntradas(ControlDirectorio* control, String path);
int directorioCrearDirectoriosRestantes(ControlDirectorio* control, String rutaDirectorio);
void directorioCrearEntradas(ControlDirectorio* control, String rutaDirectorio);
void directorioActualizar(ControlDirectorio* control, String rutaDirectorio);
bool directorioExiste(int idPadre, String nuevoNombre);
int directorioObtenerIdentificador(String path);
Directorio* directorioBuscarEnLista(int identificadorDirectorio);
bool directorioTieneAlgunArchivo(int identificador);
bool directorioTieneAlgunDirectorio(int identificador);
bool directorioTieneAlgo(int identificador);
void directorioEliminar(int identificador);
int directorioCrearConPersistencia(int identificador, String nombre, int identificadorPadre);
void directorioCrearMetadata(Entero identificador);
void directorioEliminarMetadata(Entero identificador);
bool directorioEsHijoDe(Directorio* hijo, Directorio* padre);
bool directorioOrdenarPorIdentificador(Directorio* unDirectorio, Directorio* otroDirectorio);
void directorioRecuperarPersistencia();
void directorioListaAgregar(Directorio* directorio);
int directorioListaCantidad();
Directorio* directorioListaObtener(int posicion);
void directorioListaCrear();
void directorioListaDestruir();
void directorioIniciarControl();

//--------------------------------------- Funciones de Archivo -------------------------------------

Archivo* archivoBuscarPorRuta(String path);
void archivoDestruir(Archivo* archivo);
void archivoPersistir(Archivo* metadata);
Archivo* archivoCrear(String nombreArchivo, int idPadre, String tipo);
void archivoDestruir(Archivo* archivo);
bool archivoExiste(int idPadre, String nombre);
int archivoListaPosicion(Archivo* archivo);
void archivoPersistirEliminarBloque(Archivo* archivo, int numeroBloque, int numeroCopia);
void archivoPersistirControl();
bool archivoOrdenarPorNombre(Archivo* unArchivo, Archivo* otroArchivo);
void archivoRecuperarPersistencia();
void archivoRecuperarPersistenciaDetallada(String nombre, int padre);
bool archivoBinario(Archivo* archivo);
int archivoLeer(Comando* comando);
int archivoAlmacenar(Comando* comando);
int archivoCantidadBloques(String ruta);
bool archivoDisponible(Archivo* archivo);
Archivo* archivoListaObtener(int indice);
void archivoListaCrear();
void archivoListaAgregar(Archivo* archivo);
int archivoListaCantidad();
bool archivoListaTodosDisponibles();
void archivoListaDestruir();
BloqueYama* archivoConvertirParaYama(Archivo* archivo, Entero idMaster);
void archivoEnviarBloquesYama(Puntero path);

//--------------------------------------- Funciones de Nodo -------------------------------------

Nodo* nodoCrear(Puntero datos, Socket nuevoSocket);
void nodoPersistir();
void nodoLimpiarLista();
void nodoDestruir(Nodo* nodo);
void nodoRecuperarPersistencia();
void nodoPersistirBitmap(Nodo* nodo);
void nodoFormatear(Nodo* nodo);
void nodoFormatearConectados();
void nodoVerificarBloquesLibres(Nodo* nodo);
bool nodoTieneBloquesLibres(Nodo* nodo);
bool nodoOrdenarPorBloquesLibres(Nodo* unNodo, Nodo* otroNodo);
int nodoBuscarBloqueLibre(Nodo* nodo);
Nodo* nodoBuscarPorNombre(String nombre);
void nodoRecuperarPersistenciaBitmap(Nodo* nodo);
Nodo* nodoBuscarPorSocket(Socket unSocket);
int nodoListaPosicion(Nodo* nodo);
int nodoBloquesLibresTotales();
bool nodoOrdenarPorActividad(Nodo* unNodo, Nodo* otroNodo);
void nodoLimpiarActividades();
bool nodoConectado(Nodo* nodo);
Nodo* nodoListaObtener(int posicion);
int nodoListaCantidad();
void nodoListaAgregar(Nodo* nodo);
Nodo* nodoActualizar(Nodo* nodoTemporal);
bool nodoInvalido(Nodo* nodoTemporal);
bool nodoEstaConectado(Nodo* nuevoNodo);
void nodoListaEliminar(Nodo* nodo);
void nodoListaDestruir();
void nodoListaOrdenarPorActividad();
bool nodoListaAlgunoDisponible();
void nodoAceptar(Nodo* nodo);
void nodoListaCrear();
bool nodoDisponible(Nodo* nodo);

//--------------------------------------- Funciones de Bloque -------------------------------------

Bloque* bloqueCrear(int bytes, int numero);
void bloqueDestruir(Bloque* bloque);
void bloqueCopiarEnNodo(Bloque* bloque, Nodo* nodo, Entero numeroBloqueNodo);
int bloqueEnviarANodo(Bloque* bloque, Nodo* nodo, String buffer);
int bloqueEnviarCopiasANodos(Bloque* bloque, String buffer);
bool bloqueOrdenarPorNumero(Bloque* unBloque, Bloque* otroBloque);
void bloqueCopiar(Puntero datos);
void bloqueLeer(Servidor* servidor, Puntero datos);
//void bloqueLeer(Servidor* servidor, Puntero datos, Socket unSocket);
void bloqueCopiarTexto(Servidor* servidor, Puntero datos);
void bloqueCopiarBinario(Servidor* servidor, Puntero datos);
bool bloqueDisponible(Bloque* bloque);
BloqueNodo* bloqueNodoCrear(Entero numeroBloque, String buffer, int tamanioUtilizado);
BloqueYama bloqueConvertirParaYama(Bloque* bloque);

//--------------------------------------- Funciones de Copia -------------------------------------

Copia* copiaCrear(int numeroBloqueDelNodo, String nombreNodo);
void copiaEliminar(Copia* copia);
void copiaDestruir(Copia* copia);
bool copiaDisponible(Copia* copia);
bool copiaOrdenarPorActividadDelNodo(Copia* unaCopia, Copia* otraCopia);

//--------------------------------------- Funciones de Metadata -------------------------------------

void metadataCrear();
void metadataEliminar();
void metadataIniciar();
void metadataRecuperar();

//--------------------------------------- Funciones de Ruta ------------------------------------

String rutaObtenerUltimoNombre(String ruta);
bool rutaBarrasEstanSeparadas(String ruta);
String* rutaSeparar(String ruta);
bool rutaTieneAlMenosUnaBarra(String ruta);
bool rutaValida(String ruta);
void rutaBufferCrear();
void rutaBufferDestruir();
void rutaYamaDecente(Comando* comando, int indice);
bool rutaTienePrefijoYama(String ruta);

//--------------------------------------- Funciones de Estado ------------------------------------

bool estadoEjecucionIgualA(int estado);
bool estadoFileSystemIgualA(int estado);
bool estadoControlIgualA(int estado);
void estadoControlActivar();
void estadoFileSystemEstable();
void estadoFileSystemInestable();
void estadoEjecucionNormal();
void estadoControlDesactivar();
void estadoMensaje(int estado);

//--------------------------------------- Funciones de Bitmaps de directorios ------------------------------------

void bitmapDirectoriosDestruir();
void bitmapDirectoriosCrear();
bool bitmapDirectoriosBitOcupado(int indice);
void bitmapDirectoriosOcuparBit(int posicion);

//--------------------------------------- Funciones de Semaforo ------------------------------------

void semaforosCrear();
void semaforosIniciar();
void semaforosDestruir();
