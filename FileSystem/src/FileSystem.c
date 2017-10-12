/*
 ============================================================================
 Name        : FileSystem.c
 Author      : Dario Poma
 Version     : 1.0
 Copyright   : Todos los derechos reservados papu
 Description : Proceso File System
 ============================================================================
 */

#include "FileSystem.h"

int main(int argc, String* argsv) {
	fileSystemIniciar(argsv[1]);
	fileSystemCrearConsola();
	fileSystemAtenderProcesos();
	fileSystemFinalizar();
	return EXIT_SUCCESS;
}

//--------------------------------------- Funciones de File System -------------------------------------

void testCabecita() {
	Archivo* metadata = memoriaAlocar(sizeof(Archivo));
	metadata->identificadorPadre = 99;
	metadata->listaBloques = listaCrear();
	stringCopiar(metadata->nombre, "Manco");
	stringCopiar(metadata->tipo, "TEXTO");
	Bloque* bloque = memoriaAlocar(sizeof(Bloque));
	bloque->bytes = 1014;
	bloque->listaCopias = listaCrear();
	CopiaBloque* copia = memoriaAlocar(sizeof(CopiaBloque));
	copia->bloqueNodo = 14;
	stringCopiar(copia->nombreNodo, "NODO1");
	CopiaBloque* copia2 = memoriaAlocar(sizeof(CopiaBloque));;
	copia2->bloqueNodo = 15;
	stringCopiar(copia2->nombreNodo, "NODO2");
	Bloque* bloque2 = memoriaAlocar(sizeof(Bloque));
	bloque2->bytes = 101;
	bloque2->listaCopias = listaCrear();
	CopiaBloque* copia3 = memoriaAlocar(sizeof(CopiaBloque));;
	copia3->bloqueNodo = 99;
	stringCopiar(copia3->nombreNodo, "NODO1");
	listaAgregarElemento(bloque->listaCopias, copia);
	listaAgregarElemento(bloque->listaCopias, copia2);
	listaAgregarElemento(bloque2->listaCopias, copia3);
	listaAgregarElemento(metadata->listaBloques, bloque);
	listaAgregarElemento(metadata->listaBloques, bloque2);
	archivoPersistir(metadata);
}

void fileSystemIniciar(String flag) {
	pantallaLimpiar();
	imprimirMensajeProceso("# PROCESO FILE SYSTEM");
	archivoLog = archivoLogCrear(RUTA_LOG, "FileSystem");
	imprimirMensaje(archivoLog, "[EJECUCION] Proceso File System inicializado");
	fileSystemActivar();
	archivoConfigObtenerCampos();
	configuracion = configuracionCrear(RUTA_CONFIG, (Puntero)configuracionLeerArchivoConfig, campos);
	configuracionImprimir(configuracion);
	listaNodos = listaCrear();
	if(stringIguales(flag, "--clean")) {
		comandoFormatearFileSystem();
		directoriosDisponibles = 100;
	}
	else {
		imprimirMensaje(archivoLog, "[ESTADO] Recuperando estado anterior");
		ArchivoConfig archivoNodo = config_create(RUTA_NODOS);
		int indice;
		int nodosConectados = archivoConfigEnteroDe(archivoNodo, "NODOS_CONECTADOS");
		for(indice = 0; indice < nodosConectados; indice++) {
			String campo = memoriaAlocar(30);
			stringCopiar(campo, "NOMBRE_NODO");
			String numeroNodo = stringConvertirEntero(indice);
			stringConcatenar(&campo,numeroNodo);
			memoriaLiberar(numeroNodo);
			String nombreNodo = archivoConfigStringDe(archivoNodo, campo);
			memoriaLiberar(campo);
			campo = memoriaAlocar(30);
			stringCopiar(campo, nombreNodo);
			stringConcatenar(&campo, "_BLOQUES_TOTALES");
			int bloquesTotales = archivoConfigEnteroDe(archivoNodo, campo);
			memoriaLiberar(campo);
			campo = memoriaAlocar(30);
			stringCopiar(campo, nombreNodo);
			stringConcatenar(&campo, "_BLOQUES_LIBRES");
			int bloquesLibres = archivoConfigEnteroDe(archivoNodo, campo);
			Nodo* nodo = nodoCrear(nombreNodo, bloquesTotales, bloquesLibres, 0);
			memoriaLiberar(campo);
			listaAgregarElemento(listaNodos, nodo);
		}
		archivoConfigDestruir(archivoNodo);
	}
	listaArchivos = listaCrear();
	testCabecita();
}

void fileSystemCrearConsola() {
	hiloCrear(&hiloConsola, (Puntero)consolaAtenderComandos, NULL);
}

void fileSystemAtenderProcesos() {
	Servidor* servidor = memoriaAlocar(sizeof(Servidor));
	servidorInicializar(servidor);
	while(fileSystemActivado())
		servidorAtenderPedidos(servidor);
	servidorFinalizar(servidor);
}

void fileSystemFinalizar() {
	imprimirMensaje(archivoLog, "[EJECUCION] Finalizando proceso File System...");
	archivoLogDestruir(archivoLog);
	memoriaLiberar(configuracion);
	listaDestruirConElementos(listaDirectorios, memoriaLiberar);
	nodoLimpiarLista();
	//listaDestruir(listaArchivos);
	sleep(2);
}

//--------------------------------------- Funciones de Consola -------------------------------------


bool consolaEntradaRespetaLimiteEspacios(String entrada) {
	int indice;
	int contador = 0;
	for(indice = 0; caracterDistintos(entrada[indice], FIN); indice++)
		if(caracterIguales(entrada[indice],ESPACIO))
			contador++;
	return contador <=  4;
}

bool consolaEntradaSinEspacioEnExtremos(String entrada) {
	if(stringDistintos(entrada, VACIO))
		return caracterDistintos(entrada[0], ESPACIO) &&
				caracterDistintos(entrada[stringLongitud(entrada)-1],ESPACIO);
	else
		return false;
}

bool consolaEntradaEspaciosSeparados(String entrada) {
 	int indice;
	for(indice = 0; caracterDistintos(entrada[indice],FIN); indice++)
		if(caracterIguales(entrada[indice],ESPACIO) && (caracterIguales(entrada[indice-1],ESPACIO) ||
			caracterIguales(entrada[indice+1],ESPACIO)))
			return false;
	return true;
}


bool consolaEntradaEspaciosBienUtilizados(String entrada) {
	return consolaEntradaSinEspacioEnExtremos(entrada) &&
			consolaEntradaEspaciosSeparados(entrada);

}

bool consolaEntradaSinTabs(String entrada) {
	int indice;
	for(indice = 0; caracterDistintos(entrada[indice], FIN); indice++)
		if(caracterIguales(entrada[indice],TAB))
			return 0;
	return 1;
}

bool consolaEntradaTieneEspaciosNecesarios(String entrada) {
	return consolaEntradaRespetaLimiteEspacios(entrada) &&
			consolaEntradaSinTabs(entrada);
}

bool consolaEntradaLlena(String entrada) {
	return stringDistintos(entrada, VACIO);
}

bool consolaEntradaDecente(String entrada) {
	return consolaEntradaTieneEspaciosNecesarios(entrada) &&
			consolaEntradaEspaciosBienUtilizados(entrada) &&
			consolaEntradaLlena(entrada);
}

int consolaIdentificarComando(String comando) {
	if(stringNulo(comando))
		return ERROR;
	if(stringIguales(comando, C_FORMAT))
		return FORMAT;
	if(stringIguales(comando, C_INFO))
		return INFO;
	if(stringIguales(comando, C_RM))
		return RM;
	if(stringIguales(comando, C_RMB))
		return RMB;
	if(stringIguales(comando, C_RMD))
		return RMD;
	if(stringIguales(comando, C_RENAME))
		return RENAME;
	if(stringIguales(comando, C_MV))
		return MV;
	if(stringIguales(comando, C_CAT))
		return CAT;
	if(stringIguales(comando, C_MKDIR))
		return MKDIR;
	if(stringIguales(comando, C_CPFROM))
		return CPFROM;
	if(stringIguales(comando, C_CPTO))
		return CPTO;
	if(stringIguales(comando, C_CPBLOCK))
		return CPBLOCK;
	if(stringIguales(comando, C_MD5))
		return MD5;
	if(stringIguales(comando, C_LS))
		return LS;
	if(stringIguales(comando, C_INFO))
		return INFO;
	if(stringIguales(comando, C_RENAME))
		return RENAME;
	if(stringIguales(comando, C_EXIT))
		return EXIT;
	else
		return ERROR;
}

bool consolaComandoTipoUno(String comando) {
	return stringIguales(comando, C_RM) ||
			stringIguales(comando, C_CAT) ||
			stringIguales(comando, C_MKDIR) ||
			stringIguales(comando, C_MD5) ||
			stringIguales(comando, C_LS) ||
			stringIguales(comando, C_INFO);
}

bool consolaComandoTipoDos(String comando) {
	return stringIguales(comando, C_RENAME) ||
			stringIguales(comando, C_MV) ||
			stringIguales(comando, C_CPFROM) ||
			stringIguales(comando, C_CPTO);
}

bool consolaComandoTipoTres(String comando) {
	return stringIguales(comando, C_CPBLOCK);
}


int consolaComandoCantidadArgumentos(String comando) {
	if(consolaComandoTipoUno(comando))
		return 1;
	if(consolaComandoTipoDos(comando))
		return 2;
	if(consolaComandoTipoTres(comando))
		return 3;
	else
		return 0;
}

bool consolaComandoExiste(String comando) {
	return consolaComandoTipoUno(comando) ||
			consolaComandoTipoDos(comando) ||
			consolaComandoTipoTres(comando) ||
			stringIguales(comando, C_FORMAT) ||
			stringIguales(comando, C_EXIT);
}


bool consolaValidarComandoSinTipo(String* subcadenas) {
	return stringNulo(subcadenas[1]);
}

bool consolaValidarComandoTipoUno(String* subcadenas) {
	return stringValido(subcadenas[1]) &&
			stringNulo(subcadenas[2]);
}

bool consolaValidarComandoTipoDos(String* subcadenas) {
	return stringValido(subcadenas[1]) &&
			stringValido(subcadenas[2]) &&
		   stringNulo(subcadenas[3]);
}

bool consolaValidarComandoTipoTres(String* subcadenas) {
	return stringValido(subcadenas[1]) &&
		   stringValido(subcadenas[2]) &&
		   stringValido(subcadenas[3]) &&
		   stringNulo(subcadenas[4]);
}


bool consolaComandoEsRemoverFlag(String buffer) {
	return stringIguales(buffer, FLAG_B) ||
			stringIguales(buffer, FLAG_D);
}


bool consolaComandoRemoverFlagB(String* subcadenas) {
	return stringIguales(subcadenas[0], C_RM) &&
			stringIguales(subcadenas[1], FLAG_B);
}

bool consolaComandoRemoverFlagD(String* subcadenas) {
	return stringIguales(subcadenas[0], C_RM) &&
			stringIguales(subcadenas[1], FLAG_D);
}

bool consolaValidarComandoFlagB(String* subcadenas) {
	return stringValido(subcadenas[1]) &&
			   stringValido(subcadenas[2]) &&
			   stringValido(subcadenas[3]) &&
			   stringValido(subcadenas[4]);
}

bool consolaValidarComandoFlagD(String* subcadenas) {
	return stringValido(subcadenas[1]) &&
			   stringValido(subcadenas[2]) &&
			   stringNulo(subcadenas[3]) &&
			   stringNulo(subcadenas[4]);
}

bool consolaComandoControlarArgumentos(String* subcadenas) {
	if(consolaComandoRemoverFlagB(subcadenas))
		return consolaValidarComandoFlagB(subcadenas);
	if(consolaComandoRemoverFlagD(subcadenas))
		return consolaValidarComandoFlagD(subcadenas);
	if(consolaComandoTipoUno(subcadenas[0]))
		return consolaValidarComandoTipoUno(subcadenas);
	if(consolaComandoTipoDos(subcadenas[0]) )
		return consolaValidarComandoTipoDos(subcadenas);
	if(consolaComandoTipoTres(subcadenas[0]))
		return consolaValidarComandoTipoTres(subcadenas);
	else
		return consolaValidarComandoSinTipo(subcadenas);
}


void consolaNormalizarRemoverFlagB(String* buffer) {
	buffer[0] = stringDuplicar(C_RMB);
	buffer[1] = buffer[2];
	buffer[2] = buffer[3];
	buffer[3] = buffer[4];
	buffer[4] = NULL;
}

void consolaNormalizarRemoverFlagD(String* buffer) {
	buffer[0] = stringDuplicar(C_RMD);
	buffer[1] = buffer[2];
	buffer[2] = buffer[3];
	buffer[3] = buffer[4];
	buffer[4] = NULL;
}

void consolaNormalizarComando(String* buffer) {
	if(stringIguales(buffer[1], FLAG_B))
		consolaNormalizarRemoverFlagB(buffer);
	else
		consolaNormalizarRemoverFlagD(buffer);
}

bool consolaValidarComando(String* buffer) {
	if(consolaComandoExiste(buffer[0]))
		return consolaComandoControlarArgumentos(buffer);
	else
		return false;
}

String consolaLeerEntrada() {
	int indice;
	int caracterLeido;
	char* cadena = memoriaAlocar(MAX);
	for(indice = 0; caracterDistintos((caracterLeido= caracterObtener()),ENTER); indice++)
		cadena[indice] = caracterLeido;
	cadena[indice] = FIN;
	return cadena;
}


void consolaIniciarArgumentos(String* argumentos) {
	argumentos[0] = NULL;
	argumentos[1] = NULL;
	argumentos[2] = NULL;
	argumentos[3] = NULL;
	argumentos[4] = NULL;
}

void consolaLiberarArgumentos(String* argumentos) {
	memoriaLiberar(argumentos[0]);
	memoriaLiberar(argumentos[1]);
	memoriaLiberar(argumentos[2]);
	memoriaLiberar(argumentos[3]);
	memoriaLiberar(argumentos[4]);
}

void consolaObtenerArgumentos(String* buffer, String entrada) {
	int indice;
	int indiceBuffer = 0;
	int ultimoEspacio = -1;
	for(indice = 0; caracterDistintos(entrada[indice], FIN); indice++)
		if(caracterIguales(entrada[indice],ESPACIO)) {
			buffer[indiceBuffer] = stringTomarCantidad(entrada, ultimoEspacio+1, indice-ultimoEspacio-1);
			ultimoEspacio = indice;
			indiceBuffer++;
		}
	buffer[indiceBuffer] = stringTomarCantidad(entrada, ultimoEspacio+1, indice-ultimoEspacio-1);
}


void consolaSetearArgumentos(String* argumentos, String* buffer) {
	argumentos[0]=buffer[1];
	argumentos[1]=buffer[2];
	argumentos[2]=buffer[3];
}

void consolaConfigurarComando(Comando* comando, String entrada) {
	consolaIniciarArgumentos(comando->argumentos);
	if(consolaEntradaDecente(entrada)) {
		consolaObtenerArgumentos(comando->argumentos, entrada);
		if(consolaValidarComando(comando->argumentos))
			comando->identificador = consolaIdentificarComando(comando->argumentos[0]);
		else
			comando->identificador = ERROR;
	}
	else {
		comando->identificador = ERROR;
	}
}

void consolaEjecutarComando(Comando* comando) {
	switch(comando->identificador) {
		case FORMAT: comandoFormatearFileSystem(); break;
		case RM: comandoRemoverArchivo(comando); break;
		case RMB: comandoRemoverBloque(comando); break;
		case RMD: comandoRemoverDirectorio(comando); break;
		case RENAME: comandoRenombrarArchivoDirectorio(comando); break;
		case MV: comandoMoverArchivoDirectorio(comando); break;
		case CAT: comandoMostrarArchivo(comando); break;
		case MKDIR: comandoCrearDirectorio(comando); break;
		case CPFROM: comandoCopiarArchivoDeFS(comando); break;
		case CPTO: comandoCopiarArchivoDeYFS(comando); break;
		case CPBLOCK: comandoCopiarBloque(comando); break;
		case MD5: comandoObtenerMD5(comando); break;
		case LS: comandoListarDirectorio(comando); break;
		case INFO: comandoInformacionArchivo(comando); break;
		case EXIT: comandoFinalizar();break;
		default: comandoError(); break;
	}
}

void consolaDestruirComando(Comando* comando, String entrada) {
	consolaLiberarArgumentos(comando->argumentos);
	memoriaLiberar(entrada);
}

void consolaAtenderComandos() {
	hiloDetach(pthread_self());
	while(fileSystemActivado()) {
		char* entrada = consolaLeerEntrada();
		if(fileSystemActivado()) {
			Comando comando;
			consolaConfigurarComando(&comando, entrada);
			consolaEjecutarComando(&comando);
			consolaDestruirComando(&comando, entrada);
		}
	}
}
//--------------------------------------- Funciones de Servidor -------------------------------------

bool servidorObtenerMaximoSocket(Servidor* servidor) {
	return servidor->maximoSocket;
}

void servidorSetearListaSelect(Servidor* servidor) {
	servidor->listaSelect = servidor->listaMaster;
}

void servidorControlarMaximoSocket(Servidor* servidor, Socket unSocket) {
	if(socketEsMayor(unSocket, servidor->maximoSocket))
		servidor->maximoSocket = unSocket;
}

void servidorEsperarSolicitud(Servidor* servidor) {
	socketSelect(servidor->maximoSocket, &servidor->listaSelect);
}

bool socketEsDataNode(Servidor* servidor, Socket unSocket) {
	return listaSocketsContiene(unSocket, &servidor->listaDataNodes);
}

bool socketEsYAMA(Servidor* servidor, Socket unSocket) {
	return socketSonIguales(servidor->procesoYAMA, unSocket);
}

void servidorFinalizarConexion(Servidor* servidor, Socket unSocket) {

	bool buscarNodoPorSocket(void* unNodo) {
		Nodo* nodo = (Nodo*)unNodo;;
		return nodo->socket == unSocket;
	}

	listaSocketsEliminar(unSocket, &servidor->listaMaster);
	if(socketEsDataNode(servidor, unSocket)) {
		listaSocketsEliminar(unSocket, &servidor->listaDataNodes);
		listaEliminarDestruyendoPorCondicion(listaNodos, (Puntero)buscarNodoPorSocket, memoriaLiberar);
		imprimirMensaje(archivoLog, "[CONEXION] Un proceso Data Node se ha desconectado");
	}
	else
		imprimirMensaje(archivoLog, "[CONEXION] El proceso YAMA se ha desconectado");
	socketCerrar(unSocket);
}

void servidorRegistrarConexion(Servidor* servidor, Socket unSocket) {
	if(unSocket != ERROR) {
		listaSocketsAgregar(unSocket, &servidor->listaMaster);
		servidorControlarMaximoSocket(servidor, unSocket);
	}

}

Socket servidorAceptarYAMA(Servidor* servidor, Socket unSocket) {
	Socket nuevoSocket;
	nuevoSocket = socketAceptar(unSocket, ID_YAMA);
	if(nuevoSocket != ERROR) {
		servidor->procesoYAMA = nuevoSocket;
		imprimirMensaje(archivoLog, "[CONEXION] Proceso YAMA conectado exitosamente");
	}
	return nuevoSocket;
}

bool socketEsListenerDataNode(Servidor* servidor, Socket unSocket) {
	return socketSonIguales(servidor->listenerDataNode, unSocket);
}


void servidorAceptarConexion(Servidor* servidor, Socket unSocket) {

}

void servidorRecibirMensaje(Servidor* servidor, Socket unSocket) {
	Mensaje* mensaje = mensajeRecibir(unSocket);
	if(mensajeDesconexion(mensaje))
		servidorFinalizarConexion(servidor, unSocket);
	else {
		puts("MENSAJE");
	}


	mensajeDestruir(mensaje);
}


bool socketEsListenerYAMA(Servidor* servidor, Socket unSocket) {
	return socketSonIguales(servidor->listenerYAMA, unSocket);
}

bool socketEsListener(Servidor* servidor, Socket unSocket) {
	return socketEsListenerDataNode(servidor, unSocket) || socketEsListenerYAMA(servidor, unSocket);
}

bool socketRealizoSolicitud(Servidor* servidor, Socket unSocket) {
	return listaSocketsContiene(unSocket, &servidor->listaSelect);
}

void servidorControlarSocket(Servidor* servidor, Socket unSocket) {

}

void servidorActivarListenerYAMA(Servidor* servidor) {
	servidor->listenerYAMA = socketCrearListener(configuracion->puertoYAMA);
	listaSocketsAgregar(servidor->listenerYAMA, &servidor->listaMaster);
	servidorControlarMaximoSocket(servidor, servidor->listenerYAMA);
}

void servidorActivarListenerDataNode(Servidor* servidor) {
	servidor->listenerDataNode = socketCrearListener(configuracion->puertoDataNode);
	listaSocketsAgregar(servidor->listenerDataNode, &servidor->listaMaster);
	servidorControlarMaximoSocket(servidor, servidor->listenerDataNode);
}

void servidorActivarListeners(Servidor* servidor) {
	servidorActivarListenerDataNode(servidor);
	servidorActivarListenerYAMA(servidor);
}

void servidorInicializar(Servidor* servidor) {
	servidor->maximoSocket = 0;
	listaSocketsLimpiar(&servidor->listaMaster);
	listaSocketsLimpiar(&servidor->listaSelect);
	listaSocketsLimpiar(&servidor->listaDataNodes);
	servidorActivarListeners(servidor);
}

void servidorFinalizar(Servidor* servidor) {
	memoriaLiberar(servidor);
}

void servidorRegistrarDataNode(Servidor* servidor, Socket nuevoSocket) {
	if(nuevoSocket != ERROR) {
		listaSocketsAgregar(nuevoSocket, &servidor->listaDataNodes);
		Mensaje* mensaje = mensajeRecibir(nuevoSocket);
		Nodo* nodo = nodoCrear(mensaje->datos, 0, 0, nuevoSocket);
		mensajeDestruir(mensaje);
		listaAgregarElemento(listaNodos, nodo);
		imprimirMensaje(archivoLog, "[CONEXION] Proceso Data Node conectado exitosamente");
	}
}

void servidorAtenderPedidos(Servidor* servidor) {
	servidorSetearListaSelect(servidor);
	servidorEsperarSolicitud(servidor);
	Socket unSocket;
	Socket maximoSocket = servidor->maximoSocket;
	for(unSocket = 0; unSocket <= maximoSocket; unSocket++)
		if (socketRealizoSolicitud(servidor, unSocket)) {
			if(socketEsListener(servidor, unSocket)) {
				Socket nuevoSocket;
				if(socketEsListenerDataNode(servidor, unSocket)) {
					nuevoSocket = socketAceptar(unSocket, ID_DATANODE);
					if(fileSystemDesactivado())
						break;
					servidorRegistrarDataNode(servidor, nuevoSocket);
				}
				else
					nuevoSocket = servidorAceptarYAMA(servidor, unSocket);
				servidorRegistrarConexion(servidor, nuevoSocket);
			}
			else
				servidorRecibirMensaje(servidor, unSocket);
		}
}

//--------------------------------------- Funciones de Configuracion -------------------------------------

Configuracion* configuracionLeerArchivoConfig(ArchivoConfig archivoConfig) {
	Configuracion* configuracion = memoriaAlocar(sizeof(Configuracion));
	stringCopiar(configuracion->puertoYAMA, archivoConfigStringDe(archivoConfig, "PUERTO_YAMA"));
	stringCopiar(configuracion->puertoDataNode, archivoConfigStringDe(archivoConfig, "PUERTO_DATANODE"));
	archivoConfigDestruir(archivoConfig);
	return configuracion;
}

void configuracionImprimir(Configuracion* configuracion) {
	imprimirMensajeUno(archivoLog, "[CONEXION] Esperando conexion de YAMA (Puerto: %s)", configuracion->puertoYAMA);
	imprimirMensajeUno(archivoLog, "[CONEXION] Esperando conexiones de Data Nodes (Puerto: %s)", configuracion->puertoDataNode);
}

void archivoConfigObtenerCampos() {
	campos[0] = "PUERTO_YAMA";
	campos[1] = "PUERTO_DATANODE";
}

bool fileSystemActivado() {
	return estadoFileSystem == ACTIVADO;
}

bool fileSystemDesactivado() {
	return estadoFileSystem == DESACTIVADO;
}

void fileSystemActivar() {
	estadoFileSystem = ACTIVADO;
}

void fileSystemDesactivar() {
	estadoFileSystem = DESACTIVADO;
}


//--------------------------------------- Funciones de Comando -------------------------------------


void nodoIniciarControl() {
	fileLimpiar(RUTA_NODOS);
	nodoPersistir();
}



void directorioIniciarControl() {
	listaDestruirConElementos(listaDirectorios, memoriaLiberar);
	listaDirectorios = listaCrear();
	fileLimpiar(RUTA_DIRECTORIOS);
}


void comandoFormatearFileSystem() {
	directorioIniciarControl();
	nodoIniciarControl();
	imprimirMensaje(archivoLog, "[ESTADO] El File System ha sido formateado existosamente");
	//archivoIniciarControl();
	//bitmapIniciarControl();
}

void comandoRemoverArchivo(Comando* comando) {

}


void comandoRemoverBloque(Comando* comando) {

}

void comandoRemoverDirectorio(Comando* comando) {

}

void comandoRenombrarArchivoDirectorio(Comando* comando) {

}

void comandoMoverArchivoDirectorio(Comando* comando) {

}

void comandoMostrarArchivo(Comando* comando) {

}

void comandoCrearDirectorio(Comando* comando) {
	ControlDirectorio* control = directorioControlCrear(comando->argumentos[1]);
	if(stringDistintos(comando->argumentos[1],"/"))
		while(stringValido(control->nombreDirectorio)) {
			directorioBuscarIdentificador(control);
			directorioActualizar(control, comando->argumentos[1]);
			directorioControlSetearNombre(control);
		}
	else
		imprimirMensaje(archivoLog, "[ERROR] El directorio raiz no puede ser creado");
	int indice;
	for(indice=0; stringValido(control->nombresDirectorios[indice]); indice++)
		memoriaLiberar(control->nombresDirectorios[indice]);
	memoriaLiberar(control->nombresDirectorios);
	memoriaLiberar(control);
}

void comandoCopiarArchivoDeFS(Comando* comando) {

}

void comandoCopiarArchivoDeYFS(Comando* comando) {

}
void comandoCopiarBloque(Comando* comando) {

}
void comandoObtenerMD5(Comando* comando) {

}
void comandoListarDirectorio(Comando* comando) {

}
void comandoInformacionArchivo(Comando* comando) {

}

void comandoFinalizar() {
	 fileSystemDesactivar();
	 socketCrearCliente(IP_LOCAL, configuracion->puertoDataNode, ID_DATANODE);
}

void comandoError() {
	imprimirMensaje(archivoLog, "[ERROR] Comando invalido");
}

//--------------------------------------- Funciones de Directorio -------------------------------------

void directorioAgregarALista(Directorio* directorio) {
	listaAgregarElemento(listaDirectorios, directorio);
}

bool directorioIndiceOcupado(int indice) {
	return bitmapDirectorios[indice] == 1;
}

bool directorioIndiceRespetaLimite(int indice) {
	return indice < 100;
}

bool directorioIndiceEstaOcupado(int indice) {
	return directorioIndiceRespetaLimite(indice) && directorioIndiceOcupado(indice);
}

bool directorioExisteIdentificador(int identificador) {
	return identificador != ERROR;
}

bool directorioIndicesIguales(int unIndice, int otroIndice) {
	return unIndice == otroIndice;
}

bool directorioSonIguales(Directorio* directorio, String nombreDirectorio, int idPadre) {
	return stringIguales(directorio->nombre, nombreDirectorio) && directorioIndicesIguales(directorio->identificadorPadre, idPadre);
}

Directorio* directorioCrear(int indice, String nombre, int padre) {
	Directorio* directorio = memoriaAlocar(sizeof(Directorio));
	directorio->identificador = indice;
	directorio->identificadorPadre = padre;
	stringCopiar(directorio->nombre, nombre);
	return directorio;
}

int directorioBuscarIndiceLibre() {
	int indice;
	for(indice = 1; directorioIndiceEstaOcupado(indice); indice++);
	if(indice < 100)
		return indice;
	else
		return ERROR;
}

String directorioConfigurarEntradaArchivo(String indice, String nombre, String padre) {
	String buffer = stringCrear();
	stringConcatenar(&buffer,indice);
	stringConcatenar(&buffer, ";");
	stringConcatenar(&buffer, nombre);
	stringConcatenar(&buffer,";");
	stringConcatenar(&buffer,padre);
	stringConcatenar(&buffer,"\n");
	return buffer;
}

void directorioPersistir(Directorio* directorio){
	File archivoDirectorio = fileAbrir(RUTA_DIRECTORIOS,"a+");
	String indice = stringConvertirEntero(directorio->identificador);
	String padre = stringConvertirEntero(directorio->identificadorPadre);
	String entrada = directorioConfigurarEntradaArchivo(indice, directorio->nombre, padre);
	archivoPersistirEntrada(archivoDirectorio, entrada);
	fileCerrar(archivoDirectorio);
	memoriaLiberar(entrada);
	memoriaLiberar(indice);
	memoriaLiberar(padre);
}

void directorioBuscarIdentificador(ControlDirectorio* control) {
	Directorio* directorio;
	int indice;
	for(indice = 0; indice < listaCantidadElementos(listaDirectorios); indice++) {
		directorio = listaObtenerElemento(listaDirectorios, indice);
		if(directorioSonIguales(directorio, control->nombreDirectorio, control->identificadorPadre)) {
			control->identificadorDirectorio = directorio->identificador;
			return;
		}
	}
	control->identificadorDirectorio = ERROR;
}


int directorioIndicesACrear(String* nombresDirectorios, int indiceDirectorios) {
	int directoriosACrear = 0;
	int indiceDirectoriosNuevos;
	for(indiceDirectoriosNuevos = indiceDirectorios; stringValido(nombresDirectorios[indiceDirectoriosNuevos]); indiceDirectoriosNuevos++)
		directoriosACrear++;
	return directoriosACrear;
}

bool directorioHaySuficientesIndices(ControlDirectorio* control) {
	return directorioIndicesACrear(control->nombresDirectorios, control->indiceNombresDirectorios) <= directoriosDisponibles;
}

void directorioControlSetearNombre(ControlDirectorio* control) {
	control->nombreDirectorio = control->nombresDirectorios[control->indiceNombresDirectorios];
}

String* directorioSeparar(String ruta) {
	int indice;
	int contadorDirectorios = 0;
	for(indice = 0; ruta[indice] != FIN; indice++)
		if(caracterIguales(ruta[indice], BARRA))
			contadorDirectorios++;
	String* directorios = memoriaAlocar((contadorDirectorios+1)*sizeof(String));
	int PosicionUltimaBarra = 0;
	contadorDirectorios = 0;
	for(indice = 0; caracterDistintos(ruta[indice], FIN); indice++)
		if(caracterIguales(ruta[indice],BARRA) && indice != 0) {
			directorios[contadorDirectorios] = stringTomarCantidad(ruta, PosicionUltimaBarra+1, indice-PosicionUltimaBarra-1);
			PosicionUltimaBarra = indice;
			contadorDirectorios++;
		}
	directorios[contadorDirectorios] = stringTomarCantidad(ruta, PosicionUltimaBarra+1, indice-PosicionUltimaBarra-1);
	directorios[contadorDirectorios+1] = NULL;
	return directorios;
}

ControlDirectorio* directorioControlCrear(String rutaDirectorio) {
	ControlDirectorio* controlDirectorio = memoriaAlocar(sizeof(ControlDirectorio));
	controlDirectorio->nombresDirectorios = directorioSeparar(rutaDirectorio);
	controlDirectorio->indiceNombresDirectorios = 0;
	controlDirectorio->identificadorDirectorio = 0;
	controlDirectorio->identificadorPadre = 0;
	directorioControlSetearNombre(controlDirectorio);
	return controlDirectorio;
}

void directorioControlarEntradas(ControlDirectorio* control) {
	if(control->nombresDirectorios[control->indiceNombresDirectorios + 1] == NULL)
		imprimirMensaje(archivoLog, "[ERROR] El directorio ya existe");
	else
		control->identificadorPadre = control->identificadorDirectorio;
	control->indiceNombresDirectorios++;
}


void directorioCrearDirectoriosRestantes(ControlDirectorio* control, String rutaDirectorio) {
	while(stringValido(control->nombresDirectorios[control->indiceNombresDirectorios])) {
		int indice = directorioBuscarIndiceLibre();
		Directorio* directorio = directorioCrear(indice, control->nombresDirectorios[control->indiceNombresDirectorios], control->identificadorPadre);
		bitmapDirectorios[indice] = 1;
		directorioAgregarALista(directorio);
		control->identificadorPadre = indice;
		control->indiceNombresDirectorios++;
		directoriosDisponibles--;
		directorioPersistir(directorio);
	}
	imprimirMensajeUno(archivoLog, "[DIRECTORIO] El directorio %s fue creado exitosamente", rutaDirectorio);
}

void directorioCrearEntradas(ControlDirectorio* control, String rutaDirectorio) {
	if(directorioHaySuficientesIndices(control))
		directorioCrearDirectoriosRestantes(control, rutaDirectorio);
	else
		imprimirMensaje(archivoLog,"[ERROR] No se pudo crear el directorio por superar el limite permitido (100)");
}

void directorioActualizar(ControlDirectorio* control, String rutaDirectorio) {
	if (directorioExisteIdentificador(control->identificadorDirectorio))
		directorioControlarEntradas(control);
	else
		directorioCrearEntradas(control, rutaDirectorio);
}

void directorioLimpiarLista() {
	listaDestruirConElementos(listaDirectorios, memoriaLiberar);
}



//--------------------------------------- Funciones de Archivo -------------------------------------

Archivo* archivoCrear(String nombreArchivo, int idPadre, String tipo) {
	Archivo* archivo = memoriaAlocar(sizeof(Archivo));
	stringCopiar(archivo->nombre, nombreArchivo);
	archivo->identificadorPadre = idPadre;
	stringCopiar(archivo->tipo, tipo);
	archivo->listaBloques = listaCrear();
	return archivo;
}

Bloque* bloqueCrear(int bytes) {
	Bloque* bloque = memoriaAlocar(sizeof(Bloque));
	bloque->bytes = bytes;
	bloque->listaCopias = listaCrear();
	return bloque;
}

CopiaBloque* copiaBloqueCrear(int numeroBloqueDelNodo, String nombreNodo) {
	CopiaBloque* copiaBloque = memoriaAlocar(sizeof(CopiaBloque));
	copiaBloque->bloqueNodo = numeroBloqueDelNodo;
	stringCopiar(copiaBloque->nombreNodo, nombreNodo);
	return copiaBloque;
}

void nodoLimpiarLista() {
	listaDestruirConElementos(listaNodos, memoriaLiberar);
}

Nodo* nodoCrear(String nombre, int bloquesTotales, int bloquesLibres, Socket unSocket) {
	Nodo* nodo = memoriaAlocar(sizeof(Nodo));
	stringCopiar(nodo->nombre, nombre);
	nodo->bloquesTotales = bloquesTotales;
	nodo->bloquesLibres = bloquesLibres;
	nodo->socket = unSocket;
	return nodo;
}


void archivoPersistir(Archivo* archivo) {
	String idPadre = stringConvertirEntero(archivo->identificadorPadre);
	String ruta = memoriaAlocar(MAX_STRING);
	stringCopiar(ruta, RUTA_ARCHIVOS);
	stringConcatenar(&ruta, idPadre);
	mkdir(ruta, 0777);
	stringConcatenar(&ruta, "/");
	stringConcatenar(&ruta, archivo->nombre);
	File file = fileAbrir(ruta, "a+");
	memoriaLiberar(idPadre);
	memoriaLiberar(ruta);
	fprintf(file, "NOMBRE=%s\n", archivo->nombre);
	fprintf(file, "ID_PADRE=%i\n", archivo->identificadorPadre);
	fprintf(file, "TIPO=%s\n", archivo->tipo);
	int indice;
	for(indice = 0; indice < listaCantidadElementos(archivo->listaBloques); indice++) {
		Bloque* bloque = listaObtenerElemento(archivo->listaBloques, indice);
		fprintf(file, "BLOQUE%i_BYTES=%i\n",indice, bloque->bytes);
		int indiceCopia;
		for(indiceCopia = 0; indiceCopia < listaCantidadElementos(bloque->listaCopias); indiceCopia++) {
			CopiaBloque* copiaBloque = listaObtenerElemento(bloque->listaCopias, indiceCopia);
			fprintf(file, "BLOQUE%i_COPIA%i=[%s,%i]\n", indice, indiceCopia, copiaBloque->nombreNodo, copiaBloque->bloqueNodo);
		}
	}
	fileCerrar(file);
}

void nodoPersistir() {
	File archivo = fileAbrir(RUTA_NODOS, "a+");
	int indice;
	int contadorBloquesTotales = 0;
	int contadorBloquesLibres = 0;
	for(indice = 0; indice < listaCantidadElementos(listaNodos); indice++) {
		Nodo* unNodo = listaObtenerElemento(listaNodos, indice);
		fprintf(archivo, "NOMBRE_NODO%i=%s\n", indice, unNodo->nombre);
		fprintf(archivo, "%s_BLOQUES_TOTALES=%i\n", unNodo->nombre, unNodo->bloquesTotales);
		fprintf(archivo, "%s_BLOQUES_LIBRES=%i\n",unNodo->nombre, unNodo->bloquesLibres);
		contadorBloquesTotales+=unNodo->bloquesTotales;
		contadorBloquesLibres+=unNodo->bloquesLibres;
	}
	fprintf(archivo, "NODOS_CONECTADOS=%i\n", indice);
	fprintf(archivo, "BLOQUES_LIBRES=%i\n", contadorBloquesTotales);
	fprintf(archivo, "BLOQUES_TOTALES=%i\n", contadorBloquesLibres);
	fileCerrar(archivo);
}

/* VER PORQUE FALLA
 #include "ZPrueba.h"

void liberar(void* algo) {
	if(algo!=NULL)
	free(algo);
}

int main(void) {
	char* a = "/home/utnso/jaja";
	char** b = string_split(a, "/");
	printf("%s\n", b[0]);
	printf("%s\n", b[1]);
	printf("%s\n", b[2]);
	free(b[0]);
	free(b[1]);
	free(b[2]);
	free(a);
	return 0;
}

 */
