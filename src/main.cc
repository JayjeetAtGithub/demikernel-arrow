#define _POSIX_C_SOURCE 200809L

#include <arpa/inet.h>
#include <assert.h>
#include <demi/libos.h>
#include <demi/sga.h>
#include <demi/wait.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>

#include "compute.h"

#define DATA_SIZE 64
#define MAX_BYTES (DATA_SIZE * 1024)


static void sighandler(int signum)
{
    const char *signame = strsignal(signum);

    fprintf(stderr, "\nReceived %s signal\n", signame);
    fprintf(stderr, "Exiting...\n");

    exit(EXIT_SUCCESS);
}

/*====================================================================================================================*
 * accept_wait()                                                                                                      *
 *====================================================================================================================*/

/**
 * @brief Accepts a connection on a socket and waits for operation to complete.
 *
 * @param qd Target queue descriptor.
 *
 * @returns On successful completion, a queue descriptor for an accepted connection is returned. On failure, this
 * function aborts the program execution.
 */
static int accept_wait(int qd)
{
    demi_qtoken_t qt = -1;
    demi_qresult_t qr ;

    /* Accept a connection. */
    assert(demi_accept(&qt, qd) == 0);

    /* Wait for operation to complete. */
    assert(demi_wait(&qr, qt) == 0);

    /* Parse operation result. */
    assert(qr.qr_opcode == DEMI_OPC_ACCEPT);

    return (qr.qr_value.ares.qd);
}

/*====================================================================================================================*
 * connect_wait()                                                                                                     *
 *====================================================================================================================*/

/**
 * @brief Connects to a remote socket and waits for operation to complete.
 *
 * @param qd    Target queue descriptor.
 * @param saddr Remote socket address.
 */
static void connect_wait(int qd, const struct sockaddr_in *saddr)
{
    demi_qtoken_t qt = -1;
    demi_qresult_t qr ;

    /* Connect to remote */
    assert(demi_connect(&qt, qd, (const struct sockaddr *)saddr, sizeof(struct sockaddr_in)) == 0);

    /* Wait for operation to complete. */
    assert(demi_wait(&qr, qt) == 0);

    /* Parse operation result. */
    assert(qr.qr_opcode == DEMI_OPC_CONNECT);
}

/*====================================================================================================================*
 * push_wait()                                                                                                        *
 *====================================================================================================================*/

/**
 * @brief Pushes a scatter-gather array to a remote socket and waits for operation to complete.
 *
 * @param qd  Target queue descriptor.
 * @param sga Target scatter-gather array.
 * @param qr  Storage location for operation result.
 */
static void push_wait(int qd, demi_sgarray_t *sga, demi_qresult_t *qr)
{
    demi_qtoken_t qt = -1;

    /* Push data. */
    assert(demi_push(&qt, qd, sga) == 0);

    /* Wait push operation to complete. */
    assert(demi_wait(qr, qt) == 0);

    /* Parse operation result. */
    assert(qr->qr_opcode == DEMI_OPC_PUSH);
}

/*====================================================================================================================*
 * pop_wait()                                                                                                         *
 *====================================================================================================================*/

/**
 * @brief Pops a scatter-gather array and waits for operation to complete.
 *
 * @param qd Target queue descriptor.
 * @param qr Storage location for operation result.
 */
static void pop_wait(int qd, demi_qresult_t *qr)
{
    demi_qtoken_t qt = -1;

    /* Pop data. */
    assert(demi_pop(&qt, qd) == 0);

    /* Wait for pop operation to complete. */
    assert(demi_wait(qr, qt) == 0);

    /* Parse operation result. */
    assert(qr->qr_opcode == DEMI_OPC_POP);
    assert(qr->qr_value.sga.sga_segs != 0);
}

/*====================================================================================================================*
 * server()                                                                                                           *
 *====================================================================================================================*/

/**
 * @brief TCP echo server.
 *
 * @param argc  Argument count.
 * @param argv  Argument list.
 * @param local Local socket address.
 */
static void server(int argc, char *const argv[], struct sockaddr_in *local)
{
    int qd = -1;
    int nbytes = 0;
    int sockqd = -1;

    /* Initialize demikernel */
    assert(demi_init(argc, argv) == 0);

    /* Setup local socket. */
    assert(demi_socket(&sockqd, AF_INET, SOCK_STREAM, 0) == 0);
    assert(demi_bind(sockqd, (const struct sockaddr *)local, sizeof(struct sockaddr_in)) == 0);
    assert(demi_listen(sockqd, 16) == 0);

    /* Accept client . */
    qd = accept_wait(sockqd);

    /* Read the dataset */
    cp::ExecContext exec_ctx;
    std::shared_ptr<arrow::RecordBatchReader> reader = ScanDataset(exec_ctx, "dataset", "100").ValueOrDie();

    std::shared_ptr<arrow::RecordBatch> batch;
    reader->ReadNext(&batch);
    std::cout << batch->ToString() << std::endl;
    demi_sgarray_t arrow_sga = demi_sgaalloc(batch->num_columns());
    std::cout << arrow_sga.sga_numsegs << "\n";

    /* Run. */
    while (nbytes < MAX_BYTES)
    {
        demi_qresult_t qr;
        demi_sgarray_t sga;

        /* Pop scatter-gather array. */
        pop_wait(qd, &qr);

        /* Extract received scatter-gather array. */
        memcpy(&sga, &qr.qr_value.sga, sizeof(demi_sgarray_t));

        nbytes += sga.sga_segs[0].sgaseg_len;

        /* Push scatter-gather array. */
        push_wait(qd, &sga, &qr);

        /* Release received scatter-gather array. */
        assert(demi_sgafree(&sga) == 0);

        fprintf(stdout, "ping (%d)\n", nbytes);
    }
}

/*====================================================================================================================*
 * client()                                                                                                           *
 *====================================================================================================================*/

/**
 * @brief TCP echo client.
 *
 * @param argc   Argument count.
 * @param argv   Argument list.
 * @param remote Remote socket address.
 */
static void client(int argc, char *const argv[], const struct sockaddr_in *remote)
{
    int nbytes = 0;
    int sockqd = -1;

    /* Initialize demikernel */
    assert(demi_init(argc, argv) == 0);

    /* Setup socket. */
    assert(demi_socket(&sockqd, AF_INET, SOCK_STREAM, 0) == 0);

    /* Connect to server. */
    connect_wait(sockqd, remote);

    /* Run. */
    while (nbytes < MAX_BYTES)
    {
        demi_qresult_t qr ;
        demi_sgarray_t sga ;

        /* Allocate scatter-gather array. */
        sga = demi_sgaalloc(DATA_SIZE);
        assert(sga.sga_segs != 0);

        /* Cook data. */
        memset(sga.sga_segs[0].sgaseg_buf, 1, DATA_SIZE);

        /* Push scatter-gather array. */
        push_wait(sockqd, &sga, &qr);

        /* Release sent scatter-gather array. */
        assert(demi_sgafree(&sga) == 0);

        /* Pop data scatter-gather array. */
        memset(&qr, 0, sizeof(demi_qresult_t));
        pop_wait(sockqd, &qr);

        /* Check payload. */
        for (uint32_t i = 0; i < qr.qr_value.sga.sga_segs[0].sgaseg_len; i++)
            assert(((char *)qr.qr_value.sga.sga_segs[0].sgaseg_buf)[i] == 1);
        nbytes += qr.qr_value.sga.sga_segs[0].sgaseg_len;

        /* Release received scatter-gather array. */
        assert(demi_sgafree(&qr.qr_value.sga) == 0);

        fprintf(stdout, "pong (%d)\n", nbytes);
    }
}

static void usage(const char *progname)
{
    fprintf(stderr, "Usage: %s MODE ipv4-address port\n", progname);
    fprintf(stderr, "MODE:\n");
    fprintf(stderr, "  --client    Run in client mode.\n");
    fprintf(stderr, "  --server    Run in server mode.\n");
}

void build_sockaddr(const char *const ip_str, const char *const port_str, struct sockaddr_in *const addr)
{
    int port = -1;

    sscanf(port_str, "%d", &port);
    addr->sin_family = AF_INET;
    addr->sin_port = htons(port);
    assert(inet_pton(AF_INET, ip_str, &addr->sin_addr) == 1);
}

int main(int argc, char *const argv[])
{
    /* Install signal handlers. */
    signal(SIGINT, sighandler);
    signal(SIGQUIT, sighandler);
    signal(SIGTSTP, sighandler);

    if (argc >= 4)
    {
        struct sockaddr_in saddr ;

        /* Build addresses.*/
        build_sockaddr(argv[2], argv[3], &saddr);

        /* Run. */
        if (!strcmp(argv[1], "--server"))
            server(argc, argv, &saddr);
        else if (!strcmp(argv[1], "--client"))
            client(argc, argv, &saddr);

        return (EXIT_SUCCESS);
    }

    usage(argv[0]);

    return (EXIT_SUCCESS);
}