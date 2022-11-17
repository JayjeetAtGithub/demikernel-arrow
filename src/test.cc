#define _POSIX_C_SOURCE 200809L

#include <iostream>

#include "utils.h"
#include "compute.h"

// 2 = OK

static void respond_ok(int qd) {
    demi_sgarray_t sga = demi_sgaalloc(1);
    demi_qresult_t qr;
    memset(sga.sga_segs[0].sgaseg_buf, 2, 1);
    push_wait(qd, &sga, &qr);
    assert(demi_sgafree(&sga) == 0);
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
    std::shared_ptr<arrow::RecordBatchReader> reader = 
        ScanDataset(exec_ctx, "dataset", "100").ValueOrDie();
    std::shared_ptr<arrow::RecordBatch> batch;
    arrow::Status s;
    demi_sgarray_t sga;
    demi_qresult_t qr;
    
    while (true) {
        pop_wait(qd, &qr);

        if (qr.qr_value.sga.sga_segs[0].sgaseg_len == 0) {
            break;
        }

        if (qr.qr_value.sga.sga_segs[0].sgaseg_len > MAX_REQ_SIZE) {
            std::cout << "Error: Request size too large to process." << std::endl;
            exit(EXIT_FAILURE);
        }

        assert(qr.qr_value.sga.sga_segs[0].sgaseg_len == MAX_REQ_SIZE);

        char req = *((char *)qr.qr_value.sga.sga_segs[0].sgaseg_buf);
        std::cout << "Received request: " << req << std::endl;

        if (req == 'a') {
            s = reader->ReadNext(&batch);
            if (!s.ok() || batch == nullptr) {
                sga = demi_sgaalloc(10);
                memset(sga.sga_segs[0].sgaseg_buf, 1, 10);
                push_wait(qd, &sga, &qr);
                assert(demi_sgafree(&sga) == 0);
            } else {
                respond_ok(qd); 
            }
        } else if (req == 'b') {
            
        }

        // demi_sgarray_t sga = demi_sgaalloc(DATA_SIZE);
        // assert(sga.sga_segs != 0);
        // memset(sga.sga_segs[0].sgaseg_buf, 1, DATA_SIZE);

        // push_wait(qd, &sga, &qr);
        // assert(demi_sgafree(&sga) == 0);
    }


    // while (nbytes < MAX_BYTES)
    // {
    //     demi_qresult_t qr;
    //     demi_sgarray_t sga;

    //     /* Pop scatter-gather array. */
    //     pop_wait(qd, &qr);

    //     /* Extract received scatter-gather array. */
    //     // fprintf(stdout, "size %d", sizeof(demi_sgarray_t));
    //     // memcpy(&sga, &qr.qr_value.sga, sizeof(demi_sgarray_t));
    //     sga = demi_sgaalloc(DATA_SIZE);
    //     assert(sga.sga_segs != 0);
    //     memset(sga.sga_segs[0].sgaseg_buf, 1, DATA_SIZE);

    //     nbytes += sga.sga_segs[0].sgaseg_len;

    //     /* Push scatter-gather array. */
    //     push_wait(qd, &sga, &qr);

    //     /* Release received scatter-gather array. */
    //     assert(demi_sgafree(&sga) == 0);

    //     fprintf(stdout, "ping (%d)\n", nbytes);
    // }
}

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
    while (nbytes < 1024*1024)
    {
        demi_qresult_t qr;
        demi_sgarray_t sga;

        /* Allocate scatter-gather array. */
        sga = demi_sgaalloc(1);
        assert(sga.sga_segs != 0);

        /* Cook request data. */
        memcpy(sga.sga_segs[0].sgaseg_buf, "a", 1);

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

int main(int argc, char *const argv[])
{
    /* Install signal handlers. */
    signal(SIGINT, sighandler);
    signal(SIGQUIT, sighandler);
    signal(SIGTSTP, sighandler);

    if (argc >= 4)
    {
        struct sockaddr_in saddr = {0};

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