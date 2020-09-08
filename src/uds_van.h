#ifndef _PS_UDS_VAN_H_
#define _PS_UDS_VAN_H_

#include <cstdio>
#include <cstdlib>
#include <string>
#include <string.h>
#include <unordered_map>
#include <mutex>
#include <sys/socket.h>  
#include <sys/un.h>  
#include <unistd.h>  
#include "ps/internal/van.h"
#include "ps/ps.h"

namespace ps {

const char *file_name = "UDS.socket";

class UDSVan : public Van {
public:
	UDSVan() {}
	~UDSVan() {}

protected:
	void Start(int customer_id) override {
		CHECK_LT(socket_fd = socket(AF_UNIX, SOCK_STREAM, 0), 0);	
		Van::Start(customer_id);		
	}

	void Stop() override {
		PS_VLOG(1) << my_node_.ShortDebugString() << " is stopping";
   	 	Van::Stop();
		close(socket_fd);
	}

	int Bind(const Node& node, int max_retry) override {
		struct sockaddr_un un;
		int len;
		
		un.sun_family = AF_UNIX;
		strcpy(un.sun_path, file_name);
		len = offsetof(struct sockaddr_un, sun_path) + strlen(file_name);
		unlink(file_name);
		
		CHECK_LT(bind(socket_fd, (struct sockaddr *)&un, len), 0);

		if(node.role == Node::SERVER) {
		}
		
	}

	void Connect(const Node& node) override { }

	int SendMsg(const Message& msg) override { }

	int RecvMsg(Message* msg) override {}
	

private:
	int socket_fd;
	std::mutex mu_;
	std::unordered_map<int, int> senders_;
};

}
#endif

