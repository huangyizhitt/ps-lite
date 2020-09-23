#ifndef _SHMVAN_H_
#define _SHMVAN_H_

#include <unordered_map>
#include <signal.h>
#include <pthread.h>
#include <sys/types.h>
#include "ps/internal/van.h"

namespace ps {

struct RingBuffer {
	unsigned char *server_buffer;
	unsigned char *client_buffer;
	unsigned int size;
	unsigned int in;
	unsigned int out;
	pthread_spinlock_t lock;
};

struct ChildInfo {
	int pid;
	int recv_size;
	int meta_size;
	int server_id;
	int sender;
	int recever;
};

struct VanBuf {
	int flag;
	int server_pid;	
	int node_id;
	int client_pid[64];
	int client_size[64];
	ChildInfo client_info[64];
//	RingBuf ring_buf[];
};

class SHMVAN : public Van{
public:
	SHMVAN() : connect_num(0) {}
	virtual ~SHMVAN() {}

	virtual void Start(int customer_id);
	virtual void Stop();
	virtual int Bind(const Node& node, int max_retry);
	virtual void Connect(const Node& node);
	void Listen();
	virtual int RecvMsg(Message *msg);
	virtual int SendMsg(const Message &msg);
	int GetConnectNum() const {return connect_num;}

	ssize_t Recv(const int node_id, void *buf, size_t len, bool is_server);
	ssize_t Send(const int node_id, const void *buf, size_t len, bool is_server);
	void Broadcast(const void *buf, size_t len, bool is_server);

private:
	static void SignalConnectHandle(int signo, siginfo_t *resdata, void *unknowp);
	static void SignalRecvHandle(int signo);
	void SetCurVan();
	void SetConnectRingbuffer(int client_id, int client_node_id);

	int shmid;
	int pid;
	int node_id;																
	unsigned int connect_num;
	VanBuf *buf;
	static SHMVAN *cur_van;

	std::unordered_map<int, int> connect_id;												//connect_id[node.id]=server_id
	std::unordered_map<int, int> connect_pid;												//connect_pid[server_id] = server_pid
	std::unordered_map<int, std::pair<int, VanBuf *> > connect_buf;							//recorde <shmid, VanBuf>
	std::unordered_map<int, std::pair<int, RingBuffer *> > connect_server_ringbuffer;		//this node is client node, recorde <shmid, server ringbuffer>;
	std::unordered_map<int, std::pair<int, RingBuffer *> > connect_client_ringbuffer;		//this node is server node, recorde <shmid, client ringbuffer>;
};

}
#endif


