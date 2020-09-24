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
	int recving_threadid;
	int recv_size;
	int meta_size;
	int node_id;
};

//all node create a van buf in Bind, record this node info and client info connected this nod
struct VanBuf {
	int flag;
	int pid;			
	int recving_threadid;	
	int shm_node_id;
	ChildInfo client_info[64];
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
	virtual void *Receiving(void *args);

private:
	static void SignalHandle(int signo, siginfo_t *resdata, void *unknowp);
	
	static void SignalRecvHandle(int signo);
	void SetCurVan();
	void SetConnectRingbuffer(int client_shm_node_id);
	void Notify(int pid, int signo, int vals, bool is_thread);
	void SignalConnect(int client_shm_node_id);
	void SignalRecv(int node_id);

	int shmid;
	int pid;
	pthread_t tid;
	pthread_mutex_t mutex;	
	pthread_cond_t cond;
	int recving_threadid;
	int shm_node_id;																		//used to generate key, allocate by script															
	unsigned int connect_num;
	int sender;
	VanBuf *buf;
	static SHMVAN *cur_van;

	std::unordered_map<int, int> c_id_map;													//node client id -> client shm node id(when this node is server)  
	std::unordered_map<int, int> s_id_map;													//node server id -> server shm node id(when this node is clent)
	std::unordered_map<int, std::pair<int, VanBuf *> > 	connect_buf;						//recorde <shmid, VanBuf>
	std::unordered_map<int, std::pair<int, RingBuffer *> > connect_server_ringbuffer;		//this node is client node, recorde <shmid, server ringbuffer>;
	std::unordered_map<int, std::pair<int, RingBuffer *> > connect_client_ringbuffer;		//this node is server node, recorde <shmid, client ringbuffer>;
};

}
#endif


