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


static void push_wait(int qd, demi_sgarray_t *sga, demi_qresult_t *qr)
{
    demi_qtoken_t qt = -1;
    assert(demi_push(&qt, qd, sga) == 0);
    assert(demi_wait(qr, qt) == 0);
    assert(qr->qr_opcode == DEMI_OPC_PUSH);
}

static void pop_wait(int qd, demi_qresult_t *qr)
{
    demi_qtoken_t qt = -1;
    assert(demi_pop(&qt, qd) == 0);
    assert(demi_wait(qr, qt) == 0);
    assert(qr->qr_opcode == DEMI_OPC_POP);
    assert(qr->qr_value.sga.sga_segs != 0);
}

static void server(int argc, char *const argv[], struct sockaddr_in *local)
{
    int qd = -1;
    int nbytes = 0;
    int sockqd = -1;

    assert(demi_init(argc, argv) == 0);

    assert(demi_socket(&sockqd, AF_INET, SOCK_STREAM, 0) == 0);
    assert(demi_bind(sockqd, (const struct sockaddr *)local, sizeof(struct sockaddr_in)) == 0);
    assert(demi_listen(sockqd, 16) == 0);

    qd = accept_wait(sockqd);

    cp::ExecContext exec_ctx;
    std::shared_ptr<arrow::RecordBatchReader> reader = ScanDataset(exec_ctx, "dataset", "100").ValueOrDie();

    std::shared_ptr<arrow::RecordBatch> batch;
    if (reader->ReadNext(&batch).ok() && batch != nullptr) {

        std::cout << batch->ToString() << std::endl;
        
        int64_t num_cols = batch->num_columns();
        for (int64_t i = 0; i < num_cols; i++) {
            
            std::shared_ptr<arrow::Array> col_arr = batch->column(i);
            arrow::Type::type type = col_arr->type_id();
            int64_t null_count = col_arr->null_count();

            if (is_binary_like(type)) {
                std::shared_ptr<arrow::Buffer> data_buff = 
                    std::static_pointer_cast<arrow::BinaryArray>(col_arr)->value_data();
                std::shared_ptr<arrow::Buffer> offset_buff = 
                    std::static_pointer_cast<arrow::BinaryArray>(col_arr)->value_offsets();

                demi_sgarray_t data_sga = demi_sgaalloc(data_buff->size());
                demi_sgarray_t offset_sga = demi_sgaalloc(offset_buff->size());

                memcpy(data_sga.sga_segs[0].sgaseg_buf, (void*)data_buff->data(), data_buff->size());
                memcpy(offset_sga.sga_segs[0].sgaseg_buf, (void*)offset_buff->data(), offset_buff->size());

                demi_qresult_t data_qr;
                push_wait(sockqd, &data_sga, &data_qr);

                demi_qresult_t offset_qr;
                push_wait(sockqd, &offset_sga, &offset_qr);

                demi_sgafree(&data_sga);
                demi_sgafree(&offset_sga);

            } else {
                std::shared_ptr<arrow::Buffer> data_buff = 
                    std::static_pointer_cast<arrow::PrimitiveArray>(col_arr)->values();
                demi_sgarray_t data_sga = demi_sgaalloc(data_buff->size());

                memcpy(data_sga.sga_segs[0].sgaseg_buf, (void*)data_buff->data(), data_buff->size());

                demi_qresult_t data_qr;
                push_wait(sockqd, &data_sga, &data_qr);

                demi_sgafree(&data_sga);
            }
        }
    }
}

static void client(int argc, char *const argv[], const struct sockaddr_in *remote)
{
    int nbatches = 0;
    int sockqd = -1;

    assert(demi_init(argc, argv) == 0);

    assert(demi_socket(&sockqd, AF_INET, SOCK_STREAM, 0) == 0);

    connect_wait(sockqd, remote);

    while (nbatches <= 10)
    {
        demi_qresult_t qr ;
        demi_sgarray_t sga ;

        memset(&qr, 0, sizeof(demi_qresult_t));
        pop_wait(sockqd, &qr);
        demi_sgafree(&qr.qr_value.sga);

        nbatches += 1;
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
    signal(SIGINT, sighandler);
    signal(SIGQUIT, sighandler);
    signal(SIGTSTP, sighandler);

    if (argc >= 4)
    {
        struct sockaddr_in saddr ;

        build_sockaddr(argv[2], argv[3], &saddr);

        if (!strcmp(argv[1], "--server"))
            server(argc, argv, &saddr);
        else if (!strcmp(argv[1], "--client"))
            client(argc, argv, &saddr);

        return (EXIT_SUCCESS);
    }

    usage(argv[0]);

    return (EXIT_SUCCESS);
}
