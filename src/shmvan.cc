#include <signal.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <unistd.h>
#include <cstdio>
#include <cstring>
#include <string>
#include <signal.h>
#include <atomic>
#include "shmvan.h"
#include "ps/internal/van.h"

#define SIGCONNECT	40
#define SIGCONNECTED	(SIGCONNECT+1)
#define SIGSEND			(SIGCONNECT+2)
#define SIGRECV			(SIGCONNECT+3)

#define BIND_FLAGS	0x12345678

#define is_power_of_2(x) ((x) != 0 && (((x) & ((x) - 1)) == 0))
#define min(x,y) ({ typeof(x) _x = (x); typeof(y) _y = (y); (void) (&_x == &_y); _x < _y ? _x : _y; })
#define max(x,y) ({ typeof(x) _x = (x); typeof(y) _y = (y); (void) (&_x == &_y); _x > _y ? _x : _y; })
#define mb() asm volatile("mfence" ::: "memory");
#define rmb() asm volatile("lfence" ::: "memory");
#define wmb() asm volatile("sfence" ::: "memory");
#define smp_mb()        mb()
#define smp_rmb()       rmb()
#define smp_wmb()       wmb()
#define MB(x)		((x) << 20)

#define TRANSFER_SIZE	(1 << 20)

ps::SHMVAN* ps::SHMVAN::cur_van = NULL;
static pthread_t tid;
static pthread_mutex_t mutex;
static pthread_cond_t cond;
std::atomic<bool> recv_flag(false);

namespace ps {

typedef void (* Handle)(int, siginfo_t *, void *);

static void RegisterSignal(int signo, Handle handle)
{
	struct sigaction act;
        
    sigemptyset(&act.sa_mask);
    act.sa_sigaction=handle;
	act.sa_flags=SA_SIGINFO;

	sigaction(signo, &act, NULL);
}

static void SignalDefaultHandle(int sig)
{
	return;
}

static bool IsFileExist(const char *path)
{
	if(!path) {
		return false;
	} 

	if(access(path, F_OK)==0) {
		return true;
	}
	return false;
}

static void CreateBufferFile(std::string &file_name, int id)
{
	file_name = std::to_string(id);	

	if(!IsFileExist(file_name.c_str())) {
		creat(file_name.c_str(), 0755);
	}
}

static void CreateBufferFile(std::string &file_name, int server_id, int client_id)
{
	file_name = std::to_string(server_id) + std::to_string(client_id);	

	if(!IsFileExist(file_name.c_str())) {
		creat(file_name.c_str(), 0755);
	}
}


//this ringbuffer is used kfifo of linux kernel
static struct RingBuffer *RingbufferCreate(std::string& buffer_path, unsigned int size, int& shmid, int id, bool is_server)
{
	struct RingBuffer *ring_buffer = NULL;
	if(!is_power_of_2(size)) {
		printf("size must be power of 2\n");
		return ring_buffer;
	}
	
	int ringbuffer_key = ftok(buffer_path.c_str(), id);
	if(ringbuffer_key == -1) {
        perror("ftok fail!\n");
        return ring_buffer;
	}

	shmid = shmget(ringbuffer_key, sizeof(struct RingBuffer)+size, IPC_CREAT | 0777);
	if(shmid == -1) {
		perror("shmget fail!\n");
		return ring_buffer;
	}

	ring_buffer = (struct RingBuffer *)shmat(shmid, NULL, 0);
	if(!ring_buffer) {
		perror("shmat fail!\n");
		return ring_buffer;
	}

	if(is_server) {
		ring_buffer->server_buffer = (unsigned char *)ring_buffer + sizeof(struct RingBuffer);
		ring_buffer->size = size;
		ring_buffer->in = 0;
		ring_buffer->out = 0;
		pthread_spin_init(&ring_buffer->lock, PTHREAD_PROCESS_SHARED);
	} else {
		ring_buffer->client_buffer = (unsigned char *)ring_buffer + sizeof(struct RingBuffer);
	}
	return ring_buffer;
}

static void RingBufferDestroy(struct RingBuffer *ring_buffer, int shmid)
{
	if(!ring_buffer) return;
	shmdt(ring_buffer);
	shmctl(shmid, IPC_RMID, NULL);
}

static unsigned int __RingBufferPut(struct RingBuffer *ring_buffer, const unsigned char *buffer, unsigned int len, bool is_server)
{
	unsigned int l;

	len = min(len, ring_buffer->size - ring_buffer->in + ring_buffer->out);
	if(len == 0) return len;
	smp_mb();

	l = min(len, ring_buffer->size - (ring_buffer->in & (ring_buffer->size - 1)));

	if(is_server) {
		std::memcpy(ring_buffer->server_buffer + (ring_buffer->in & (ring_buffer->size - 1)), buffer, l);
		std::memcpy(ring_buffer->server_buffer, buffer + l, len - l);
	} else {
		std::memcpy(ring_buffer->client_buffer + (ring_buffer->in & (ring_buffer->size - 1)), buffer, l);
		std::memcpy(ring_buffer->client_buffer, buffer + l, len - l);
	}
	
	smp_wmb();
	ring_buffer->in += len;
	return len;
}

static unsigned int __RingBufferGet(struct RingBuffer *ring_buffer, unsigned char *buffer, unsigned int len, bool is_server)
{
	unsigned int l;

	len = min(len, ring_buffer->in - ring_buffer->out);
	if(len == 0) return len;
	smp_rmb();

	l = min(len, ring_buffer->size - (ring_buffer->out & (ring_buffer->size - 1)));

	if(is_server) {
		std::memcpy(buffer, ring_buffer->server_buffer + (ring_buffer->out & (ring_buffer->size - 1)), l);
		std::memcpy(buffer + l, ring_buffer->server_buffer, len - l);
	} else {
		std::memcpy(buffer, ring_buffer->client_buffer + (ring_buffer->out & (ring_buffer->size - 1)), l);
		std::memcpy(buffer + l, ring_buffer->client_buffer, len - l);
	}
	
	smp_mb();
	ring_buffer->out += len;
	return len;
}

static unsigned int RingBufferPut(struct RingBuffer *ring_buffer, const unsigned char *buffer, unsigned int len, bool is_server)
{
	unsigned int ret;

	pthread_spin_lock(&ring_buffer->lock);
	ret = __RingBufferPut(ring_buffer, buffer, len, is_server);
	pthread_spin_unlock(&ring_buffer->lock);
	return ret;
}

static unsigned int RingBufferGet(struct RingBuffer *ring_buffer, unsigned char *buffer, unsigned int len, bool is_server)
{
	unsigned int ret;
	pthread_spin_lock(&ring_buffer->lock);
	ret = __RingBufferGet(ring_buffer, buffer, len, is_server);
	if(ring_buffer->in == ring_buffer->out)
		ring_buffer->in = ring_buffer->out = 0;
	pthread_spin_unlock(&ring_buffer->lock);
	return ret;
}

static inline unsigned int RingBufferSize(struct RingBuffer *ring_buffer)
{
	return ring_buffer->size;
}

static inline unsigned int RingBufferLen(struct RingBuffer *ring_buffer)
{
	return (ring_buffer->in - ring_buffer->out);
}

static inline bool RingBufferEmpty(struct RingBuffer *ring_buffer)
{
	return (ring_buffer->in - ring_buffer->out == 0);
}

static inline bool RingBufferFull(struct RingBuffer *ring_buffer)
{
	return (ring_buffer->in - ring_buffer->out == ring_buffer->size);
}


void SHMVAN::SignalHandle(int signo, siginfo_t *resdata, void *unknowp)
{
	int vals = resdata->si_value.sival_int;
	switch(signo) {
		case SIGCONNECT:
			cur_van->SignalConnect(vals);
			break;

		case SIGRECV:
			cur_van->SignalRecv(vals);
			break;

		case SIGSEND:

			break;

		default:
			break;
	}
}

//server process
void SHMVAN::SignalConnect(int client_shm_node_id)
{
	printf("Will process client connect!\n");

	if(connect_client_ringbuffer.find(client_shm_node_id) == connect_client_ringbuffer.end())
		SetConnectRingbuffer(client_shm_node_id);
	
	int client_node_id = buf->client_info[client_shm_node_id].node_id;			//Get client node.id
	c_id_map[client_node_id] = client_shm_node_id;								//this node is server, record id_map
	//wake up client process and notify build connect;
	kill(buf->client_info[client_shm_node_id].pid, SIGCONNECTED);
}

//client process
void SHMVAN::SignalRecv(int node_id)
{
	pthread_mutex_lock(&mutex);
	sender = node_id;
	recv_flag = true;
	printf("SignalRecv node_id: %d\n", node_id);
	pthread_cond_signal(&cond);
	pthread_mutex_unlock(&mutex);
}

void SHMVAN::SetCurVan()
{
	cur_van = this;
}

void SHMVAN::SetConnectRingbuffer(int client_shm_node_id)
{
	int ringbuffer_key, ringbuffer_shmid;
	struct RingBuffer *r;
	std::string buffer_path;

	CreateBufferFile(buffer_path, shm_node_id, client_shm_node_id);
	r = RingbufferCreate(buffer_path, MB(64), ringbuffer_shmid, shm_node_id, true);

	connect_client_ringbuffer[client_shm_node_id] = std::make_pair(ringbuffer_shmid, r);
	connect_num++;
}

void SHMVAN::Notify(int pid, int signo, int vals, bool is_thread)
{
	union sigval sigvalue;
	sigvalue.sival_int = vals;
	if(is_thread) {
		pthread_sigqueue(pid, signo, sigvalue);
	} else {
		sigqueue(pid, signo, sigvalue);
	}
}

void* SHMVAN::Receiving(void *args)
{
	Meta nodes;
	Meta recovery_nodes;
	recovery_nodes.control.cmd = Control::ADD_NODE;
	
	while(true) {
		pthread_mutex_lock(&mutex);
		//protected sprious wakeup in multicore system
		while(!recv_flag) {
			pthread_cond_wait(&cond, &mutex);
		}
		printf("Will Receiving!\n");
		cur_van->Receiving_(nodes, recovery_nodes);
		recv_flag = false;
		pthread_mutex_unlock(&mutex);
//		if(!cur_van->IsReady()) break;
	}
}

void SHMVAN::Start(int customer_id)
{
	pid = getpid();
	SetCurVan();
	RegisterSignal(SIGCONNECT, SHMVAN::SignalHandle);	
	RegisterSignal(SIGRECV, SHMVAN::SignalHandle);
//	signal(SIGRECV, SignalDefaultHandle);
	signal(SIGCONNECTED, SignalDefaultHandle);
	SetSHMVan();
	pthread_cond_init(&cond, NULL);
	pthread_mutex_init(&mutex, NULL);
	pthread_create(&tid, NULL, Receiving, NULL);
	printf("Will begin start!\n");
	Van::Start(customer_id);
}

int SHMVAN::Bind(const Node& node, int max_retry)
{
	int key;
	int port = node.port;
	shm_node_id = node.shm_id;

	key = ftok("/tmp", shm_node_id);
	if(key == -1) {
		perror("ftok fail!\n");
		return -1;
	}

	shmid = shmget(key, sizeof(struct VanBuf), IPC_CREAT | 0777);

	buf = (struct VanBuf *)shmat(shmid, NULL, 0);
	if(buf == NULL) {
		printf("shmat fail!\n");
		return -1;
	}

	buf->pid = pid;
	buf->shm_node_id = shm_node_id;
	buf->flag = BIND_FLAGS;

	printf("Bind success, pid: %d\n", pid);
	return port;
}

void SHMVAN::Connect(const Node& node) 
{
    CHECK_NE(node.port, node.kEmpty);
	int server_key, server_shmid, ringbuffer_key, ringbuffer_shmid;
	struct VanBuf *p;
	struct RingBuffer *r;
	std::string buffer_path;
	int server_shm_node_id = node.shm_id;

	if(shm_node_id == server_shm_node_id) {
		printf("Connect self is not nessecery!\n");
		return;
	}

	if(connect_buf.find(server_shm_node_id) != connect_buf.end()) {
		printf("The node has connected!\n");
		
		//node.id is assigned by scheduler after van->start(), so should update the my_node_.id
 		p = connect_buf[server_shm_node_id].second;
		if(my_node_.id != p->client_info[shm_node_id].node_id) {
			p->client_info[shm_node_id].node_id = my_node_.id;

			//notify server update new client_node.id
			Notify(p->pid, SIGCONNECT, shm_node_id, false);
			pause();				//wait build connect;
		}
		
		return;
	} 
	
	server_key = ftok("/tmp", server_shm_node_id);
	if(server_key == -1) {
		perror("ftok fail!\n");
		return;
	}

	server_shmid = shmget(server_key, sizeof(struct VanBuf), IPC_CREAT | 0777);
	p = (struct VanBuf *)shmat(server_shmid, NULL, 0);
	printf("server_key: %d, server_shmid: %d\n", server_key, server_shmid);
	while(p->flag != BIND_FLAGS);				//wait server LISTEN
	p->client_info[shm_node_id].pid = pid;
	p->client_info[shm_node_id].recving_threadid = recving_threadid;
	p->client_info[shm_node_id].node_id = my_node_.id;
	connect_buf[server_shm_node_id] = std::make_pair(server_shmid, p);
	s_id_map[node.id] = server_shm_node_id;										//this node is client, record id map
	
	//send shm_node_id to server by sigqueue and build connect
	Notify(p->pid, SIGCONNECT, shm_node_id, false);
	pause();				//wait build connect;

	CreateBufferFile(buffer_path, server_shm_node_id, shm_node_id);
	r = RingbufferCreate(buffer_path, MB(64), ringbuffer_shmid, server_shm_node_id, false);
	connect_server_ringbuffer[server_shm_node_id] = std::make_pair(ringbuffer_shmid, r);
	unlink(buffer_path.c_str());

	printf("Connected client %d <--> server %d\n", shm_node_id, server_shm_node_id);
}

void SHMVAN::Stop()
{
	Van::Stop();
	pthread_join(tid, NULL);
	//delete connect buf
	for(const auto& n : connect_buf) {
		shmdt(n.second.second);
		shmctl(n.second.first, IPC_RMID, NULL);
	}

	//delete ringbuffer, only can be deleted by server
	for(const auto& n : connect_client_ringbuffer) {
//		shmdt(n.second.second);
//		shmctl(n.second.first, IPC_RMID, NULL);
		RingBufferDestroy(n.second.second, n.second.first);
	}

	//delete self buf
	shmdt(buf);
	shmctl(shmid, IPC_RMID, NULL);
	
	connect_buf.clear();
	connect_server_ringbuffer.clear();
	connect_client_ringbuffer.clear();
}

ssize_t SHMVAN::Recv(const int node_id, void *buf, size_t len, bool is_server)
{
	ssize_t l = 0, _l;
	struct RingBuffer *ring_buffer = NULL;
	
	if(len == 0) return l;

	if(is_server) {
		if(connect_client_ringbuffer.find(node_id)!=connect_client_ringbuffer.end())
			ring_buffer = connect_client_ringbuffer[node_id].second;
	} else {
		if(connect_server_ringbuffer.find(node_id)!=connect_server_ringbuffer.end())
			ring_buffer = connect_server_ringbuffer[node_id].second;
	}

	if(!ring_buffer) return -1;

	while(len > TRANSFER_SIZE) {
		_l = RingBufferGet(ring_buffer, (unsigned char *)buf+l, TRANSFER_SIZE, is_server);
		l += _l;
		len -= _l;
	}

	while(len > 0) {
		_l = RingBufferGet(ring_buffer, (unsigned char *)buf+l, len, is_server);
		l += _l;
		len -= _l;
	}

	return l;
}

ssize_t SHMVAN::Send(const int node_id, const void *buf, size_t len, bool is_server)
{
	ssize_t l = 0, _l;
	struct RingBuffer *ring_buffer = NULL;

	if(len == 0) return l;

	if(is_server) {
		if(connect_client_ringbuffer.find(node_id)!=connect_client_ringbuffer.end())
			ring_buffer = connect_client_ringbuffer[node_id].second;
	} else {
		if(connect_server_ringbuffer.find(node_id)!=connect_server_ringbuffer.end())
			ring_buffer = connect_server_ringbuffer[node_id].second;
	}

	if(!ring_buffer) {
		printf("Node %d has no send ringbuffer!\n", node_id);
		return -1;
	}
	
	while(len > TRANSFER_SIZE) {
		_l = RingBufferPut(ring_buffer, (unsigned char *)buf+l, TRANSFER_SIZE, is_server);
		l += _l;
		len -= _l;
	}
	
	while(len > 0) {
		_l = RingBufferPut(ring_buffer,(unsigned char *)buf+l, len, is_server);
		l += _l;
		len -= _l;
	}
	printf("Send: size %d bytes\n", l);	
	return l;
}

//Send msg, priority server to send data
int SHMVAN::SendMsg(const Message& msg) {
  	// find the socket
  	int id = msg.meta.recver;
  	CHECK_NE(id, Meta::kEmpty);

	bool is_server = (c_id_map.find(id) != c_id_map.end());
	
  	int meta_size; char* meta_buf;
  	PackMeta(msg.meta, &meta_buf, &meta_size);
	int size = 0;
	int n = msg.data.size();
	for(int i = 0; i < n; i++) {
		size += msg.data[i].size();
	}

	int target_id, target_pid;

	if(is_server) {
		target_id = c_id_map[id];
		target_pid = buf->client_info[target_id].pid;
		buf->client_info[target_id].recv_size = size;
		buf->client_info[target_id].meta_size = meta_size;
	} else {
		target_id = s_id_map[id];
		struct VanBuf *p = connect_buf[target_id].second;
		target_pid = p->pid;
		p->client_info[shm_node_id].recv_size = size;
		p->client_info[shm_node_id].meta_size = meta_size;
	}

	printf("SendMsg: my node: %d, my mode shm: %d, target id: %d, target pid: %d\n", my_node_.id, shm_node_id, target_id, target_pid);
	Notify(target_pid, SIGRECV, my_node_.id, false);
	//send meta
	while (true) {
		if (Send(target_id, meta_buf, meta_size, is_server) == meta_size) break;
		printf("WARNING failed to send meta data to node: %d, send size: %d\n", id, meta_size);
		return -1;
	}
	delete meta_buf;
	printf("Send meta success, size: %d, control cmd: %d, control node size: %d\n", meta_size, msg.meta.control.cmd, msg.meta.control.node.size());
  	int send_bytes = meta_size;

	// send data
  	for (int i = 0; i < n; ++i) {
		SArray<char>* data = new SArray<char>(msg.data[i]);
		int data_size = data->size();
		
		while (true) {
	  		if (Send(target_id, data, data_size, is_server) == data_size) break;
	  		printf("WARNING failed to send meta data to node: %d, send size: %d\n", id, data_size);
	  		return -1;
		}
	
		send_bytes += data_size;
  }
  return send_bytes;
}

//Recv msg, priority client to recv data
int SHMVAN::RecvMsg(Message* msg) 
{
	int target_id, meta_size, recv_counts;
	struct VanBuf *p;
	size_t recv_bytes = 0;
	msg->data.clear();

	bool is_client = (s_id_map.find(sender) != s_id_map.end());
	if(is_client) {
		target_id = s_id_map[sender];
		p = connect_buf[target_id].second;
		recv_bytes = p->client_info[shm_node_id].recv_size;
		meta_size = p->client_info[shm_node_id].meta_size;
	} else {
		target_id = c_id_map[sender];
		recv_bytes = buf->client_info[target_id].recv_size;
		meta_size = buf->client_info[target_id].meta_size;
	}

	printf("RecvMsg is_client: %d, my shm node: %d, target_shm_id: %d\n", is_client, shm_node_id, target_id);
	msg->meta.sender = (sender == target_id + 10000) ? Meta::kEmpty : sender;			//sender == target_id + 10000 is in init stage, node don't have global ID
    	msg->meta.recver = my_node_.id;

	char *meta_buf = (char *)malloc(meta_size);
	
	recv_counts = Recv(target_id, meta_buf, meta_size, (!is_client));
	if(recv_counts != meta_size) {
		printf("Recv meta data fail!\n");
		free(meta_buf);
		return -1;
	}

	UnpackMeta(meta_buf, meta_size, &(msg->meta));
	free(meta_buf);
	
	printf("Receive meta success, size: %d, control cmd is: %d, control node size: %d\n", recv_counts, msg->meta.control.cmd, msg->meta.control.node.size());
	char *recv_buf = (char *)malloc(recv_bytes);
	recv_counts = Recv(target_id, recv_buf, recv_bytes, (!is_client));
	if(recv_counts != recv_bytes) {
		printf("Recv data fail!\n");
		free(recv_buf);
		return -1;
	}

	return (recv_counts + meta_size);
}

}

