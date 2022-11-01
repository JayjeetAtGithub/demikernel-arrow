// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

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
#include <chrono>
#include <thread>

#define DATA_SIZE 1024
#define MAX_BYTES DATA_SIZE * 1024 * 1024

static void sighandler(int signum)
{
    const char *signame = strsignal(signum);

    fprintf(stderr, "\nReceived %s signal\n", signame);
    fprintf(stderr, "Exiting...\n");

    exit(EXIT_SUCCESS);
}

void reg_sighandlers()
{
    signal(SIGINT, sighandler);
    signal(SIGQUIT, sighandler);
    signal(SIGTSTP, sighandler);
}

static int accept_wait(int qd)
{
    demi_qtoken_t qt = -1;
    demi_qresult_t qr ;
    assert(demi_accept(&qt, qd) == 0);
    assert(demi_wait(&qr, qt) == 0);
    assert(qr.qr_opcode == DEMI_OPC_ACCEPT);
    return (qr.qr_value.ares.qd);
}

static void connect_wait(int qd, const struct sockaddr_in *saddr)
{
    demi_qtoken_t qt = -1;
    demi_qresult_t qr ;
    assert(demi_connect(&qt, qd, (const struct sockaddr *)saddr, sizeof(struct sockaddr_in)) == 0);
    assert(demi_wait(&qr, qt) == 0);
    assert(qr.qr_opcode == DEMI_OPC_CONNECT);
}

// static void push_wait(int qd, demi_sgarray_t *sga, demi_qresult_t *qr)
static void push_wait(int qd, demi_sgarray_t *sga)
{
    demi_qtoken_t qt = -1;
    assert(demi_push(&qt, qd, sga) == 0);
    // assert(demi_wait(qr, qt) == 0);
    // fprintf(stdout, "push_wait: (%d)\n", qr->qr_opcode);
    // assert(qr->qr_opcode == DEMI_OPC_PUSH);
}

static void pop_wait(int qd, demi_qresult_t *qr)
{
    demi_qtoken_t qt = -1;
    assert(demi_pop(&qt, qd) == 0);
    assert(demi_wait(qr, qt) == 0);
    assert(qr->qr_opcode == DEMI_OPC_POP);
    assert(qr->qr_value.sga.sga_segs != 0);
}

static void server(int argc, char *const argv[], const struct sockaddr_in *local)
{
    int qd = -1;
    int sockqd = -1;
    demi_qresult_t qr;

    assert(demi_init(argc, argv) == 0);
    assert(demi_socket(&sockqd, AF_INET, SOCK_STREAM, 0) == 0);
    assert(demi_bind(sockqd, (const struct sockaddr *)local, sizeof(struct sockaddr_in)) == 0);
    assert(demi_listen(sockqd, 16) == 0);
    qd = accept_wait(sockqd);

    int64_t recv_bytes = 0;

    while (recv_bytes < MAX_BYTES)
    {
        pop_wait(qd, &qr);
        fprintf(stdout, "pop: total bytes received: (%ld)\n", recv_bytes);
        recv_bytes += qr.qr_value.sga.sga_segs[0].sgaseg_len;
    }

    assert(recv_bytes == MAX_BYTES);
}

static void client(int argc, char *const argv[], const struct sockaddr_in *remote)
{
    int sockqd = -1;
    // demi_qresult_t qr;
    assert(demi_init(argc, argv) == 0);
    assert(demi_socket(&sockqd, AF_INET, SOCK_STREAM, 0) == 0);
    connect_wait(sockqd, remote);

    int64_t sent_bytes = 0;
    while (sent_bytes < MAX_BYTES)
    {
        demi_sgarray_t sga;
        sga = demi_sgaalloc(DATA_SIZE);
        memset(sga.sga_segs[0].sgaseg_buf, 1, DATA_SIZE);
        // push_wait(sockqd, &sga, &qr);
        push_wait(sockqd, &sga);
        sent_bytes += sga.sga_segs[0].sgaseg_len;
        fprintf(stdout, "push: total bytes sent: (%ld)\n", sent_bytes);
    }

    assert(sent_bytes == MAX_BYTES);
    std::this_thread::sleep_for(std::chrono::milliseconds(100000));

}

static void usage(const char *const progname)
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

// Exercises a one-way direction communication through TCP. This system-level test instantiates two demikernel peers: a
// client and a server. The client sends TCP packets to the server in a tight loop. The server process is a tight loop
// received TCP packets from the client.
int main(int argc, char *const argv[])
{
    /* Install signal handlers. */
    signal(SIGINT, sighandler);
    signal(SIGQUIT, sighandler);
    signal(SIGTSTP, sighandler);

    if (argc >= 4)
    {
        struct sockaddr_in saddr;

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
