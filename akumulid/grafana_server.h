//Grafana Interface for Akumulid
//Emulating InfluxDB 0.9.x
//(c) 2016 Claudius Zingerli
//Apache License

#pragma once
#include <string>
#include <memory>
#include <tuple>

#include <microhttpd.h>

#include "logger.h"
#include "akumuli.h"
#include "server.h"

namespace Akumuli {
namespace Grafana {

struct AccessControlList {};  // TODO(Lazin): implement ACL

struct GrafanaServer : std::enable_shared_from_this<GrafanaServer>, Server
{
    AccessControlList                       acl_;
    std::shared_ptr<ReadOperationBuilder>   proc_;
    unsigned short                          port_;
    MHD_Daemon                             *daemon_;

    GrafanaServer(unsigned short port, std::shared_ptr<ReadOperationBuilder> qproc);
    GrafanaServer(unsigned short port, std::shared_ptr<ReadOperationBuilder> qproc, AccessControlList const& acl);

    virtual void start(SignalHandler* handler, int id);
    void stop();
};

}
}

