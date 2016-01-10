#include "collectd_server.h"
#include "collectd_protoparser.h"

#include <thread>

#include <netinet/ip.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>

#include <boost/bind.hpp>

namespace Akumuli {

CollectdServer::CollectdServer(std::shared_ptr<IngestionPipeline> pipeline, int nworkers, int port, const std::string &p_typesdb_path)
    : pipeline_(pipeline)
    , start_barrier_(nworkers + 1)
    , stop_barrier_(nworkers + 1)
    , stop_{0}
    , port_(port)
    , nworkers_(nworkers)
    , typesdb_path_(p_typesdb_path)
    , logger_("CollectdServer", 128)
{
}


void CollectdServer::start(SignalHandler *sig, int id) {
    auto self = shared_from_this();
    sig->add_handler(boost::bind(&CollectdServer::stop, std::move(self)), id);

    auto logger = &logger_;
    auto error_cb = [logger](aku_Status status, uint64_t counter) {
        const char* msg = aku_error_message(status);
        logger->error() << msg;
    };

    //Parse types.db
    typesdb_.load(typesdb_path_);
    typesdb_.lock();

    // Create workers
    for (int i = 0; i < nworkers_; i++) {
        auto spout = pipeline_->make_spout();
        spout->set_error_cb(error_cb);
        std::thread thread(std::bind(&CollectdServer::worker, shared_from_this(), spout));
        thread.detach();
    }
    start_barrier_.wait();
}


void CollectdServer::stop() {
    stop_.store(1, std::memory_order_relaxed);
    stop_barrier_.wait();
    logger_.info() << "Collectd server stopped";
}


#define USE_COLLECTDPARSE_COFUNC 0
//#define USE_COLLECTDPARSE_COFUNC 1

void CollectdServer::worker(std::shared_ptr<PipelineSpout> spout) {
    start_barrier_.wait();

    int sockfd, retval;
    sockaddr_in sa;

#if USE_COLLECTDPARSE_COFUNC
    CollectdProtoParserCofunc parser(spout);
#else
    CollectdProtoParser parser(spout,std::make_shared<const TypesDB>(typesdb_));
#endif

    try {

#if USE_COLLECTDPARSE_COFUNC
        parser.start();
#endif

        // Create socket
        sockfd = socket(AF_INET, SOCK_DGRAM, 0);
        if (sockfd == -1) {
            const char* msg = strerror(errno);
            std::stringstream fmt;
            fmt << "can't create socket: " << msg;
            std::runtime_error err(fmt.str());
            BOOST_THROW_EXCEPTION(err);
        }

        // Set socket options
        int optval = 1;
        if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEPORT, &optval, sizeof(optval)) == -1) {
            const char* msg = strerror(errno);
            std::stringstream fmt;
            fmt << "can't set socket options: " << msg;
            std::runtime_error err(fmt.str());
            BOOST_THROW_EXCEPTION(err);
        }

        timeval tval;
        tval.tv_sec = 0;
        tval.tv_usec = 1000;  // 1ms
        if (setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, &tval, sizeof(tval)) == -1) {
            const char* msg = strerror(errno);
            std::stringstream fmt;
            fmt << "can't set socket timeout: " << msg;
            std::runtime_error err(fmt.str());
            BOOST_THROW_EXCEPTION(err);
        }

        // Bind socket to port
        sa.sin_family = AF_INET;
        sa.sin_addr.s_addr = htonl(INADDR_ANY);
        sa.sin_port = htons(port_);

        if (bind(sockfd, (sockaddr *) &sa, sizeof(sa)) == -1) {
            const char* msg = strerror(errno);
            std::stringstream fmt;
            fmt << "can't bind socket: " << msg;
            std::runtime_error err(fmt.str());
            BOOST_THROW_EXCEPTION(err);
        }

        auto iobuf = std::make_shared<IOBuf>();

        while(!stop_.load(std::memory_order_relaxed)) {
	    //Receive at least 1 and up to NPACKETS (512) UDP packets
            retval = recvmmsg(sockfd, iobuf->msgs, NPACKETS, MSG_WAITFORONE, nullptr);
            if (retval == -1) {
                if (errno == EAGAIN || errno == EINTR) {
                    continue;
                }
                const char* msg = strerror(errno);
                std::stringstream fmt;
                fmt << "socket read error: " << msg;
                std::runtime_error err(fmt.str());
                BOOST_THROW_EXCEPTION(err);
            }

            iobuf->pps++;

	    //Process each packet
            for (int i = 0; i < retval; i++) {
                // reset buffer to receive new message
                iobuf->bps += iobuf->msgs[i].msg_len;
                size_t mlen = iobuf->msgs[i].msg_len;
                iobuf->msgs[i].msg_len = 0; //CMZ: Why?
		//Packet data goes to iobuf->msgs[i].msg_hdr.msg_iov[].iov_base
		//msg_iov[0].iov_base == iobuf->bufs[i], a char[MSS] array (MSS=2048-128)

                // parse message content
                PDU pdu = {
                    std::shared_ptr<Byte>(iobuf, iobuf->bufs[i]),
                    mlen,
                    0u,
                };

                parser.parse_next(pdu);

#if 0
                std::stringstream fmt;
                fmt << "Terminating after one packet";
                std::runtime_error err(fmt.str());
                BOOST_THROW_EXCEPTION(err);
#endif
            }
            if (retval != 0) {
                iobuf = std::make_shared<IOBuf>();
            }
        }
    } catch(...) {
        logger_.error() << boost::current_exception_diagnostic_information();
    }

#if USE_COLLECTDPARSE_COFUNC
    parser.close();
#endif

    stop_barrier_.wait();
}


struct CollectdServerBuilder {

    CollectdServerBuilder() {
        ServerFactory::instance().register_type("COLLECTD", *this);
    }

    std::shared_ptr<Server> operator () (std::shared_ptr<IngestionPipeline> pipeline,
                                         std::shared_ptr<ReadOperationBuilder>,
                                         const ServerSettings& settings) {
        return std::make_shared<CollectdServer>(pipeline, settings.nworkers, settings.port, settings.path);
    }
};

static CollectdServerBuilder reg_type;


}

