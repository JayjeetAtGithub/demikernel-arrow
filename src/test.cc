#define _POSIX_C_SOURCE 200809L

#include <iostream>

#include "utils.h"
#include "compute.h"


static void respond_finish(int qd) {
    demi_sgarray_t sga = demi_sgaalloc(1);
    demi_qresult_t qr;
    memcpy(sga.sga_segs[0].sgaseg_buf, "x", 1);
    push_wait(qd, &sga, &qr);
    assert(demi_sgafree(&sga) == 0);
}

static void respond_data(int qd, const uint8_t* buf, size_t size) {
    assert(size <= DATA_SIZE);
    demi_sgarray_t sga = demi_sgaalloc(size);
    demi_qresult_t qr;
    memcpy(sga.sga_segs[0].sgaseg_buf, buf, size);
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

        arrow::Status s = reader->ReadNext(&batch);
        if (!s.ok() || batch == nullptr) {
            respond_finish(qd);
            std::cout << "Finished sending data." << std::endl;
        } else {
            std::cout << "Number of rows: " << batch->num_rows() << std::endl;
            auto options = arrow::ipc::IpcWriteOptions::Defaults();
            std::shared_ptr<arrow::Buffer> buffer = 
                arrow::ipc::SerializeRecordBatch(*batch, options).ValueOrDie();
            if (buffer->size() <= DATA_SIZE) {
                respond_data(qd, buffer->data(), buffer->size());
            } else {
                int bytes_remaining = 0;
                while (bytes_remaining > 0) {
                    int bytes_to_send = std::min(bytes_remaining, DATA_SIZE);
                    respond_data(qd, buffer->data() + buffer->size() - bytes_remaining, bytes_to_send);
                    bytes_remaining -= bytes_to_send;
                }
            }
        }
    }
}

static void client(int argc, char *const argv[], const struct sockaddr_in *remote)
{
    int nbytes = 0;
    int sockqd = -1;

    assert(demi_init(argc, argv) == 0);
    assert(demi_socket(&sockqd, AF_INET, SOCK_STREAM, 0) == 0);
    connect_wait(sockqd, remote);

    while (true)
    {
        demi_qresult_t qr;
        demi_sgarray_t sga;

        sga = demi_sgaalloc(1);
        assert(sga.sga_segs != 0);

        memcpy(sga.sga_segs[0].sgaseg_buf, "a", 1);

        push_wait(sockqd, &sga, &qr);

        assert(demi_sgafree(&sga) == 0);

        memset(&qr, 0, sizeof(demi_qresult_t));
        pop_wait(sockqd, &qr);

        char req = *((char *)qr.qr_value.sga.sga_segs[0].sgaseg_buf);
        assert(demi_sgafree(&qr.qr_value.sga) == 0);
        if (req == 'x') {
            break;
        }
    }
}

int main(int argc, char *const argv[])
{
    signal(SIGINT, sighandler);
    signal(SIGQUIT, sighandler);
    signal(SIGTSTP, sighandler);

    if (argc >= 4)
    {
        struct sockaddr_in saddr = {0};
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
