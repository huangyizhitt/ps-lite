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
	int meta_size;
	int node_id;
	int total_recv_size;			//total data size: data size + meta size
	int recv_counts;				//data count
	int recv_size[1024];			//data[0] + data[1] + ... + data[recv_counts];
};

//all node create a van buf in Bind, record this node info and client info connected this nod
struct VanBuf {
	int flag;
	int pid;			
	int connector;
	int sender_identity;	
	int shm_node_id;
	pthread_spinlock_t lock;					//protect connector
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

	int RecvMsg1(Message *msg);
	int SendMsg1(const Message &msg);
	ssize_t Recv(const int node_id, void *buf, size_t len, bool is_server);
	ssize_t Send(const int node_id, const void *buf, size_t len, bool is_server);
	void Broadcast(const void *buf, size_t len, bool is_server);
	static void *Receiving(void *args);

	void PackMeta(const Meta& meta, char **meta_buf, int *buf_size);
	void UnpackMeta(const char* meta_buf, int buf_size, Meta* meta);
	
private:
	static void SignalHandle(int signo);
	static void* SignalThread(void *args);
	
	static void SignalRecvHandle(int signo);
	void SetCurVan();
	void SetConnectRingbuffer(int client_shm_node_id);
	void Notify(int signo, struct VanBuf *buf, int vals);
	void Notify(int signo, int pid, struct VanBuf *buf, int vals, int send_identity);
	void SignalConnect();
	void SignalRecv();
	void SignalConnected();
	void WaitConnect();

	int shmid;
	int pid;
	int recving_threadid;
	int shm_node_id;																		//used to generate key, allocate by script	
	unsigned int connect_num;
	int sender;
	bool sender_identity;
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


