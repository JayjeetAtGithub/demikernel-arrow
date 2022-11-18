#define _POSIX_C_SOURCE 200809L

#include <iostream>

#include "utils.h"
#include "compute.h"


static demi_qresult_t request_control(int qd) {
    demi_sgarray_t sga = demi_sgaalloc(1);
    demi_qresult_t qr;
    memcpy(sga.sga_segs[0].sgaseg_buf, "c", 1);
    push_wait(qd, &sga, &qr);
    assert(demi_sgafree(&sga) == 0);
    memset(&qr, 0, sizeof(demi_qresult_t));
    pop_wait(qd, &qr);
    return qr;
}

static demi_qresult_t request_data(int qd) {
    demi_sgarray_t sga = demi_sgaalloc(1);
    demi_qresult_t qr;
    memcpy(sga.sga_segs[0].sgaseg_buf, "d", 1);
    push_wait(qd, &sga, &qr);
    assert(demi_sgafree(&sga) == 0);
    memset(&qr, 0, sizeof(demi_qresult_t));
    pop_wait(qd, &qr);
    return qr;
}

static void respond_finish(int qd) {
    demi_sgarray_t sga;
    demi_qresult_t qr;
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

static void server(int argc, char *const argv[], struct sockaddr_in *local) {
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
        ScanDataset(exec_ctx, "dataset", "1").ValueOrDie();
    std::shared_ptr<arrow::RecordBatch> batch;
    demi_sgarray_t sga;
    demi_qresult_t qr;
    arrow::Status s;
    std::shared_ptr<arrow::Buffer> buffer;
    int32_t bytes_remaining = 0;

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

        if (req == 'c') {
            s = reader->ReadNext(&batch);
            std::cout << "Read batch" << std::endl;
            if (!s.ok() || batch == nullptr) {
                std::cout << "Finished sending dataset." << std::endl;
                respond_finish(qd);
                break;
            }
            buffer = arrow::ipc::SerializeRecordBatch(*batch, arrow::ipc::IpcWriteOptions::Defaults()).ValueOrDie();
            std::cout << "Sending batch of size " << buffer->size() << std::endl;
            respond_data(qd, reinterpret_cast<const uint8_t*>(to_buf(buffer->size())), sizeof(int32_t));
            bytes_remaining = buffer->size();
        } else if (req == 'd') {
            int bytes_to_send = std::min(bytes_remaining, DATA_SIZE);
            respond_data(qd, buffer->data() + buffer->size() - bytes_remaining, bytes_to_send);
            bytes_remaining -= bytes_to_send;
        } else {
            std::cout << "Error: Invalid request." << std::endl;
            exit(EXIT_FAILURE);
        }
    }
}

static void client(int argc, char *const argv[], const struct sockaddr_in *remote) {
    int nbytes = 0;
    int sockqd = -1;

    assert(demi_init(argc, argv) == 0);
    assert(demi_socket(&sockqd, AF_INET, SOCK_STREAM, 0) == 0);
    connect_wait(sockqd, remote);

    int32_t size = 0;
    int32_t offset = 0;
    
    // 1: control
    // 2: data
    int req_mode = 1;

    while (true) {
        if (req_mode == 1) {
            demi_qresult_t qr = request_control(sockqd);
            if (qr.qr_value.sga.sga_segs[0].sgaseg_len == 0) {
                break;
            } else {
                size = from_buf((char *)qr.qr_value.sga.sga_segs[0].sgaseg_buf);
                std::cout << "Received size: " << size << std::endl;
                req_mode = 2;
            }
        } else if (req_mode == 2) {
            demi_qresult_t qr = request_data(sockqd);
            offset += qr.qr_value.sga.sga_segs[0].sgaseg_len;
            if (offset == size) {
                offset = 0;
                size = 0;
                req_mode = 1;
            } else {
                req_mode = 2;
            }
        }
    }
}

int main(int argc, char *const argv[]) {
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
