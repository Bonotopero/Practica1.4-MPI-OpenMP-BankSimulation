/*
 * ALUMNO / EQUIPO: ANGULO DIAZ JULIO ABRAHAM - EQUIPO: 7
 * ACTIVIDAD: Practica 1.4 - Sincronizacion Cliente-Servidor en MPI con OpenMP
 */

#include <iostream>
#include <fstream>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <cstring>
#include <iomanip>
#include <limits>
#include <windows.h>
#include <mpi.h>
#include <omp.h>

using namespace std;

#define NOMBRE_EQUIPO "ANGULO DIAZ JULIO ABRAHAM - EQUIPO: 7"

// Generador de números aleatorios reentrante para hilos en Windows
inline int rand_r_win(unsigned int* seed) {
    *seed = *seed * 1103515245 + 12345;
    return (unsigned int)(*seed / 65536) % 32768;
}

struct Transaccion {
    int id_cliente;
    int id_hilo_openmp;
    int tipo_operacion;
    double monto;
    char nombre_host[64]; // Identificador único de la máquina (Host)
};

struct RespuestaServidor {
    bool aprobada;
    double saldo_resultante;
    char mensaje[100];
};

void limpiarTeclado() {
    cin.clear();
    cin.ignore(numeric_limits<streamsize>::max(), '\n');
}

const char* getNombreOperacion(int op) {
    switch (op) {
        case 1: return "Deposito";
        case 2: return "Retiro";
        case 3: return "Consulta";
        default: return "Desconocida";
    }
}

int main(int argc, char* argv[]) {
    int proporcionado;
    // Solicitar soporte de hilos en nivel SERIALIZED
    MPI_Init_thread(&argc, &argv, MPI_THREAD_SERIALIZED, &proporcionado);

    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);


    if (rank == 0) {
        cout << "=========================================================\n";
        cout << "EQUIPO / INTEGRANTES: ANGULO DIAZ JULIO ABRAHAM - EQUIPO: 7\n";
        cout << "=========================================================\n\n";
        cout.flush();
    }

    // Configurar 4 hilos OpenMP por cada proceso cliente
    omp_set_num_threads(4);

    char nombrePC[MPI_MAX_PROCESSOR_NAME];
    int longitud_nombre;
    MPI_Get_processor_name(nombrePC, &longitud_nombre);

    int max_hilos_omp = omp_get_max_threads();

    // Archivo de auditoría por cada nodo
    char nombre_archivo_log[256];
    snprintf(nombre_archivo_log, sizeof(nombre_archivo_log), "log_equipo_%s_nodo_%d.txt", nombrePC, rank);

    ofstream log_file(nombre_archivo_log, ios::out | ios::app);
    if (log_file.is_open()) {
        log_file << "=========================================================\n";
        log_file << "LOG DE AUDITORIA - PRACTICA 1.4 CLIENTE-SERVIDOR MPI+OPENMP\n";
        log_file << "Equipo: ANGULO DIAZ JULIO ABRAHAM - EQUIPO: 7\n";
        log_file << "Nodo MPI (Rank): " << rank << " | Nombre PC: " << nombrePC << "\n";
        log_file << "Hilos OpenMP Activos: " << max_hilos_omp << "\n";
        log_file << "=========================================================\n\n";
        log_file.flush();
    }

    double saldo_cuenta = 10000.00;
    int opcion = -1;

    do {
        if (rank == 0) {
            cout << "\n=========================================================\n";
            cout << " MENU - Practica 1.4: Sincronizacion Cliente-Servidor   \n";
            cout << "=========================================================\n";
            cout << "1. Iniciar Simulacion Basica (5 operaciones por cliente)\n";
            cout << "2. Iniciar Simulacion Masiva (100,000 operaciones por cliente)\n";
            cout << "3. Consultar Saldo Final del Servidor\n";
            cout << "0. Salir\n";
            cout << "Seleccione una opcion: " << flush;

            if (!(cin >> opcion)) {
                limpiarTeclado();
                opcion = -1;
            } else {
                limpiarTeclado();
            }
        }

        // Broadcast de la opción elegida a todos los nodos
        MPI_Bcast(&opcion, 1, MPI_INT, 0, MPI_COMM_WORLD);

        if (opcion == 0) break;

        if (opcion == 1 || opcion == 2) {
            long long ops_por_cliente = (opcion == 1) ? 5 : 100000;
            int detallado = (opcion == 1) ? 1 : 0;
            long long total_esperado = ops_por_cliente * (size - 1);

            double t_inicio = MPI_Wtime();

            // NODO SERVIDOR (NODO 0)
            if (rank == 0) {
                long long transacciones_procesadas = 0;

                while (transacciones_procesadas < total_esperado) {
                    Transaccion tx;
                    MPI_Status status;

                    MPI_Recv(&tx, sizeof(Transaccion), MPI_BYTE, MPI_ANY_SOURCE, 1, MPI_COMM_WORLD, &status);

                    RespuestaServidor resp;
                    resp.aprobada = true;

                    if (tx.tipo_operacion == 1) {
                        saldo_cuenta += tx.monto;
                        snprintf(resp.mensaje, sizeof(resp.mensaje), "Deposito aprobado.");
                    }
                    else if (tx.tipo_operacion == 2) {
                        if (saldo_cuenta >= tx.monto) {
                            saldo_cuenta -= tx.monto;
                            snprintf(resp.mensaje, sizeof(resp.mensaje), "Retiro aprobado.");
                        } else {
                            resp.aprobada = false;
                            snprintf(resp.mensaje, sizeof(resp.mensaje), "Rechazado: Fondos insuficientes.");
                        }
                    }
                    else if (tx.tipo_operacion == 3) {
                        snprintf(resp.mensaje, sizeof(resp.mensaje), "Consulta exitosa.");
                    }

                    resp.saldo_resultante = saldo_cuenta;

                    MPI_Send(&resp, sizeof(RespuestaServidor), MPI_BYTE, status.MPI_SOURCE, 2, MPI_COMM_WORLD);

                    transacciones_procesadas++;

                    if (detallado) {
                        cout << "[" << tx.nombre_host << "] [Proceso MPI: " << tx.id_cliente
                             << "] [Hilo OpenMP: " << tx.id_hilo_openmp << "] [Operacion: "
                             << getNombreOperacion(tx.tipo_operacion) << "] [Monto: $"
                             << fixed << setprecision(2) << tx.monto << "] [Estado: "
                             << (resp.aprobada ? "Aprobado" : "Rechazado") << "] [Saldo Final: $"
                             << resp.saldo_resultante << "]" << endl;
                    }

                    if (log_file.is_open()) {
                        log_file << "[Host: " << tx.nombre_host << "] [Cliente: " << tx.id_cliente
                                 << "] [Hilo OMP: " << tx.id_hilo_openmp << "] [Op: "
                                 << getNombreOperacion(tx.tipo_operacion) << "] [Monto: $"
                                 << fixed << setprecision(2) << tx.monto << "] [Estado: "
                                 << (resp.aprobada ? "Aprobado" : "Rechazado") << "] [Saldo: $"
                                 << resp.saldo_resultante << "]\n";
                    }
                }

                double t_fin = MPI_Wtime();

                if (detallado) {
                    cout << ">> Simulacion basica completada exitosamente." << endl;
                } else {
                    cout << "\n=========================================================\n";
                    cout << " RESUMEN PRUEBA MASIVA (100,000 Ops / Cliente)           \n";
                    cout << "=========================================================\n";
                    cout << ">> Transacciones Procesadas Totales: " << transacciones_procesadas << endl;
                    cout << ">> Saldo Final Consolidado: $" << fixed << setprecision(2) << saldo_cuenta << endl;
                    cout << ">> Tiempo Total Transcurrido: " << (t_fin - t_inicio) << " segundos." << endl;
                    cout << "=========================================================\n";
                }
            }
            // NODOS CLIENTE (NODOS 1 .. N-1)
            else {
                #pragma omp parallel
                {
                    int tid = omp_get_thread_num();
                    int num_hilos = omp_get_num_threads();

                    long long ops_por_hilo = ops_por_cliente / num_hilos;
                    long long resto = ops_por_cliente % num_hilos;
                    if (tid < resto) ops_por_hilo++;

                    unsigned int semilla = (unsigned int)(time(NULL) + rank * 1000 + tid);

                    for (long long i = 0; i < ops_por_hilo; i++) {
                        Transaccion tx;
                        tx.id_cliente = rank;
                        tx.id_hilo_openmp = tid;
                        tx.tipo_operacion = (rand_r_win(&semilla) % 3) + 1;
                        tx.monto = ((rand_r_win(&semilla) % 50) + 1) * 100.0;

                        // Asignar el nombre del Host/Máquina a la transacción
                        strncpy(tx.nombre_host, nombrePC, sizeof(tx.nombre_host) - 1);
                        tx.nombre_host[sizeof(tx.nombre_host) - 1] = '\0';

                        RespuestaServidor resp;

                        #pragma omp critical(mpi_sync_section)
                        {
                            MPI_Send(&tx, sizeof(Transaccion), MPI_BYTE, 0, 1, MPI_COMM_WORLD);
                            MPI_Recv(&resp, sizeof(RespuestaServidor), MPI_BYTE, 0, 2, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                        }

                        if (log_file.is_open()) {
                            #pragma omp critical(log_sync_section)
                            {
                                log_file << "[" << tx.nombre_host << "] [Nodo MPI: " << rank
                                         << "] [Hilo OpenMP: " << tid << "] [Op: "
                                         << getNombreOperacion(tx.tipo_operacion) << "] [Monto: $"
                                         << fixed << setprecision(2) << tx.monto << "] [Estado: "
                                         << (resp.aprobada ? "Aprobado" : "Rechazado") << "] [Saldo: $"
                                         << resp.saldo_resultante << "]\n";
                            }
                        }

                        // Pausa de control de flujo para prevenir saturación de socket/RPC
                        if (i % 500 == 0) {
                            Sleep(1);
                        }
                    }
                }
            }

            if (log_file.is_open()) log_file.flush();
        }
        else if (opcion == 3) {
            if (rank == 0) {
                cout << ">> SALDO ACTUAL EN EL SERVIDOR: $" << fixed << setprecision(2) << saldo_cuenta << endl;
            }
        }

    } while (opcion != 0);

    if (log_file.is_open()) {
        log_file << "\nFinalizando ejecucion de nodo " << rank << ".\n";
        log_file.close();
    }

    if (rank == 0) {
        cout << "\n=========================================================\n";
        cout << "EQUIPO / INTEGRANTES: ANGULO DIAZ JULIO ABRAHAM - EQUIPO: 7\n";
        cout << "=========================================================\n";
        cout.flush();
    }

    MPI_Finalize();
    return 0;
}
