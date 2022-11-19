#define _POSIX_C_SOURCE 200809L

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
    demi_sgarray_t sga = demi_sgaalloc(1);
    demi_qresult_t qr;
    memcpy(sga.sga_segs[0].sgaseg_buf, "f", 1);
    push_wait(qd, &sga, &qr);
    assert(demi_sgafree(&sga) == 0);
}

static void respond_data(int qd, const uint8_t* buf, size_t size) {
    assert(size <= 2048);
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
        ScanDataset(exec_ctx, "dataset+mem", "1").ValueOrDie();
    std::shared_ptr<arrow::RecordBatch> batch;
    demi_sgarray_t sga;
    demi_qresult_t qr;
    arrow::Status s;
    std::shared_ptr<arrow::Buffer> buffer;
    int32_t bytes_remaining = 0;
    int32_t total_data_requests = 0;

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
        assert(demi_sgafree(&qr.qr_value.sga) == 0);

        if (req == 'c') {
            s = reader->ReadNext(&batch);
            if (!s.ok() || batch == nullptr) {
                std::cout << "Finished sending dataset in " << total_data_requests << " requests." << std::endl;
                respond_finish(qd);
                break;
            }
            buffer = PackRecordBatch(batch).ValueOrDie();
            respond_data(qd, reinterpret_cast<const uint8_t*>(to_buf(buffer->size())), sizeof(int32_t));
            bytes_remaining = buffer->size();
        } else if (req == 'd') {
            total_data_requests++;
            int bytes_to_send = std::min(bytes_remaining, 2048);
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
    int req_mode = 1;
    uint8_t *buf;

    int32_t total_rows = 0;

    while (true) {
        if (req_mode == 1) {
            demi_qresult_t qr = request_control(sockqd);
            size = from_buf((char *)qr.qr_value.sga.sga_segs[0].sgaseg_buf);
            if (qr.qr_value.sga.sga_segs[0].sgaseg_len == 1) {
                char req = *((char *)qr.qr_value.sga.sga_segs[0].sgaseg_buf);
                if (req == 'f') {
                    std::cout << "Finished receiving dataset : " << total_rows << std::endl;
                    break;
                }
            }
            buf = (uint8_t *)malloc(size);
            req_mode = 2;
            assert(demi_sgafree(&qr.qr_value.sga) == 0);
        } else if (req_mode == 2) {
            demi_qresult_t qr = request_data(sockqd);
            memcpy(buf + offset, qr.qr_value.sga.sga_segs[0].sgaseg_buf, qr.qr_value.sga.sga_segs[0].sgaseg_len);
            offset += qr.qr_value.sga.sga_segs[0].sgaseg_len;
            if (offset == size) {
                auto batch = UnpackRecordBatch(buf, size).ValueOrDie();
                total_rows += batch->num_rows();
                std::cout << "Received " << total_rows << " rows." << std::endl;
                offset = 0;
                size = 0;
                req_mode = 1;
            } else {
                req_mode = 2;
            }
            assert(demi_sgafree(&qr.qr_value.sga) == 0);
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
