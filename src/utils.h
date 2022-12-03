#define _POSIX_C_SOURCE 200809L

#include <iostream>
#include <chrono>
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

#define MAX_REQ_SIZE 1
#define DATA_SIZE 1024

enum REQ_TYPE {
    REQ_TYPE_NEXT_BATCH,
    REQ_TYPE_NEXT_COLUMN,
    REQ_TYPE_NEXT_PACKET
};

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

static void sighandler(int signum)
{
    const char *signame = strsignal(signum);

    fprintf(stderr, "\nReceived %s signal\n", signame);
    fprintf(stderr, "Exiting...\n");

    exit(EXIT_SUCCESS);
}

static int accept_wait(int qd)
{
    demi_qtoken_t qt = -1;
    demi_qresult_t qr;

    /* Accept a connection. */
    assert(demi_accept(&qt, qd) == 0);

    /* Wait for operation to complete. */
    assert(demi_wait(&qr, qt) == 0);

    /* Parse operation result. */
    assert(qr.qr_opcode == DEMI_OPC_ACCEPT);

    return (qr.qr_value.ares.qd);
}

static void connect_wait(int qd, const struct sockaddr_in *saddr)
{
    demi_qtoken_t qt = -1;
    demi_qresult_t qr;

    /* Connect to remote */
    assert(demi_connect(&qt, qd, (const struct sockaddr *)saddr, sizeof(struct sockaddr_in)) == 0);

    /* Wait for operation to complete. */
    assert(demi_wait(&qr, qt) == 0);

    /* Parse operation result. */
    assert(qr.qr_opcode == DEMI_OPC_CONNECT);
}

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

static char* to_buf(int32_t val) {
    char *buf = (char*)malloc(sizeof(int32_t));
    buf[0] = (val >> 24) & 0xFF;
    buf[1] = (val >> 16) & 0xFF;
    buf[2] = (val >> 8) & 0xFF;
    buf[3] = val & 0xFF;
    return buf;
}

static int32_t from_buf(char* buf) {
    int32_t val = 0;
    val |= (buf[0] & 0xFF) << 24;
    val |= (buf[1] & 0xFF) << 16;
    val |= (buf[2] & 0xFF) << 8;
    val |= buf[3] & 0xFF;
    return val;
}
