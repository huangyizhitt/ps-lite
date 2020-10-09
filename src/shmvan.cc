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
#include <errno.h>
#include <sys/time.h>
#include "shmvan.h"
#include "./meta.pb.h"
#include "ps/internal/van.h"

namespace ps {

#define SIGCONNECT	40
#define SIGCONNECTED	(SIGCONNECT+1)
#define SIGSEND			(SIGCONNECT+2)
#define SIGRECV			(SIGCONNECT+3)
#define SIGTERMINATE	(SIGCONNECT+4)

#define TRANSFER_SIZE	(1 << 20)

#define BIND_FLAG		0x01234567

#define is_power_of_2(x) ((x) != 0 && (((x) & ((x) - 1)) == 0))
#define min(x,y) ({ typeof(x) _x = (x); typeof(y) _y = (y); (void) (&_x == &_y); _x < _y ? _x : _y; })
#define max(x,y) ({ typeof(x) _x = (x); typeof(y) _y = (y); (void) (&_x == &_y); _x > _y ? _x : _y; })
#define mb() asm volatile("mfence" ::: "memory");
#define rmb() asm volatile("lfence" ::: "memory");
#define wmb() asm volatile("sfence" ::: "memory");
#define smp_mb()        mb()
#define smp_rmb()       rmb()
#define smp_wmb()       wmb()

SHMVAN *SHMVAN::cur_van = NULL;

template <typename T>
void Queue<T>::Push(T val)
{
	std::unique_lock <std::mutex> lck(mtx);
	q.push(std::move(val));
	cv.notify_all();
}

template <typename T>
T Queue<T>::WaitAndPop()
{
	std::unique_lock <std::mutex> lck(mtx);
	while(q.empty()) {
		cv.wait(lck);
	}
	T ret = std::move(q.front());
	q.pop();
	return ret;
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

static void CreateBufferFile(std::string &file_name, int sender_pid, int recver_pid)
{
	file_name = std::to_string(sender_pid) + std::to_string(recver_pid);	

	if(!IsFileExist(file_name.c_str())) {
		creat(file_name.c_str(), 0755);
	}
}

static void DestroyBufferFile(int sender_pid, int recver_pid)
{
	std::string file_name = std::to_string(sender_pid) + std::to_string(recver_pid);
	if(IsFileExist(file_name.c_str())) {
		unlink(file_name.c_str());
	}
}
//this ringbuffer is used kfifo of linux kernel
static struct VanBuf *VanBufCreate(std::string& buffer_path, int& shmid)
{
	struct VanBuf *buf = NULL;
	
	int key = ftok(buffer_path.c_str(), 0);
	if(key == -1) {
        perror("ftok fail!\n");
        return buf;
	}

	shmid = shmget(key, sizeof(struct VanBuf), IPC_CREAT | 0777);
	if(shmid == -1) {
		perror("shmget fail!\n");
		return buf;
	}

	buf = (struct VanBuf *)shmat(shmid, NULL, 0);
	if(!buf) {
		perror("shmat fail!\n");
		return buf;
	}

	return buf;
}

static void VanBufDestroy(int shmid, struct VanBuf *buf)
{
	if(!buf) return;
	pthread_cond_destroy(&buf->connect_cond);
	pthread_mutex_destroy(&buf->connect_mutex);
	pthread_spin_destroy(&buf->rb.lock);
	shmdt(buf);
	shmctl(shmid, IPC_RMID, NULL);
}

static bool VanBufInit(struct VanBuf *buf)
{
	if(!buf) return false;
	pthread_cond_init(&buf->connect_cond, NULL);
	pthread_mutex_init(&buf->connect_mutex, NULL);
	buf->connected_flag = false;
	buf->rb.size = RINGBUFFER_SIZE;
	buf->rb.in = buf->rb.out = 0;
	pthread_spin_init(&buf->rb.lock, PTHREAD_PROCESS_SHARED);
	return true;
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

static unsigned int __RingBufferPut(struct RingBuffer *ring_buffer, const unsigned char *buffer, unsigned int len)
{
	unsigned int l;

	len = min(len, ring_buffer->size - ring_buffer->in + ring_buffer->out);
	if(len == 0) return len;
	smp_mb();

	l = min(len, ring_buffer->size - (ring_buffer->in & (ring_buffer->size - 1)));

	std::memcpy(ring_buffer->buf + (ring_buffer->in & (ring_buffer->size - 1)), buffer, l);
	std::memcpy(ring_buffer->buf, buffer + l, len - l);
	
	smp_wmb();
	ring_buffer->in += len;
	return len;
}

static unsigned int __RingBufferGet(struct RingBuffer *ring_buffer, unsigned char *buffer, unsigned int len)
{
	unsigned int l;

	len = min(len, ring_buffer->in - ring_buffer->out);
	if(len == 0) return len;
	smp_rmb();

	l = min(len, ring_buffer->size - (ring_buffer->out & (ring_buffer->size - 1)));

	std::memcpy(buffer, ring_buffer->buf + (ring_buffer->out & (ring_buffer->size - 1)), l);
	std::memcpy(buffer + l, ring_buffer->buf, len - l);

	smp_mb();
	ring_buffer->out += len;
	return len;
}

static unsigned int RingBufferPut(struct RingBuffer *ring_buffer, const unsigned char *buffer, unsigned int len)
{
	unsigned int ret;

	pthread_spin_lock(&ring_buffer->lock);
	ret = __RingBufferPut(ring_buffer, buffer, len);
	pthread_spin_unlock(&ring_buffer->lock);
	return ret;
}

static unsigned int RingBufferGet(struct RingBuffer *ring_buffer, unsigned char *buffer, unsigned int len)
{
	unsigned int ret;
	pthread_spin_lock(&ring_buffer->lock);
	ret = __RingBufferGet(ring_buffer, buffer, len);
	if(ring_buffer->in == ring_buffer->out)
		ring_buffer->in = ring_buffer->out = 0;
	pthread_spin_unlock(&ring_buffer->lock);
	return ret;
}

void SHMVAN::SignalConnect(siginfo_t *info)
{
	pid_t sender_pid = info->si_pid;
	std::string buf_path;
	int shmid;
	printf("[%s]my pid: %d, sender pid: %d\n", __FUNCTION__, pid, sender_pid);
	CreateBufferFile(buf_path, sender_pid, pid);
	struct VanBuf *buf = VanBufCreate(buf_path, shmid);

	if(buf) {
		sender[sender_pid] = std::make_pair(shmid, buf);
		Notify(SIGCONNECTED, sender_pid);
	} else {
		printf("[%s]Create Van Buf fail!\n", __FUNCTION__);
	}
}

void SHMVAN::SignalConnected(siginfo_t *info)
{
	pid_t recv_pid = info->si_pid;
	struct VanBuf *buf = recver[recv_pid].second;
	pthread_mutex_lock(&buf->connect_mutex);
	buf->connected_flag = true;
	pthread_cond_broadcast(&buf->connect_cond);
	pthread_mutex_unlock(&buf->connect_mutex);
}

void SHMVAN::SignalRecv(siginfo_t *info)
{
	pid_queue.Push(info->si_pid);
}


void SHMVAN::SignalHandle(int signo, siginfo_t *info)
{
	switch(signo) {
		case SIGCONNECT:
			cur_van->SignalConnect(info);
			break;

		case SIGCONNECTED:
			cur_van->SignalConnected(info);
			break;

		case SIGRECV:
			cur_van->SignalRecv(info);
			break;

		case SIGSEND:

			break;

		default:
			break;
	}
}

void* SHMVAN::SignalThread(void *args)
{
	sigset_t *set = (sigset_t *)args;
	int signo;
	siginfo_t info;
	
	while(1) {
		signo = sigwaitinfo(set, &info);
		if(signo >= 0) {
			if(signo == SIGTERMINATE) break;
			SignalHandle(signo, &info);
		} else {
			printf("sigwaitinfo returned err: %d; %s\n", errno, strerror(errno));
		}
	}
	printf("Stop SignalThread\n");
	return NULL;
}

void SHMVAN::SetCurVan()
{
	cur_van = this;
}

void SHMVAN::Notify(int signo, int pid)
{
	kill(pid, signo);
}

void SHMVAN::WaitConnected(struct VanBuf *buf)
{
	pthread_mutex_lock(&buf->connect_mutex);
	while(buf->connected_flag != true) {
		pthread_cond_wait(&buf->connect_cond, &buf->connect_mutex);
	}
	pthread_mutex_unlock(&buf->connect_mutex);	
}

void SHMVAN::Start(int customer_id)
{
	pid = getpid();
	SetCurVan();

	sigemptyset(&mask);
	sigaddset(&mask, SIGCONNECT);
	sigaddset(&mask, SIGCONNECTED);
	sigaddset(&mask, SIGSEND);
	sigaddset(&mask, SIGRECV);
	sigaddset(&mask, SIGTERMINATE);

	pthread_sigmask(SIG_BLOCK, &mask, NULL);
	pthread_create(&signal_tid, NULL, SignalThread, (void *)&mask);
	
	Van::Start(customer_id);
}

int SHMVAN::Bind(const Node& node, int max_retry)
{
	int port = atoi(CHECK_NOTNULL(Environment::Get()->find("DMLC_SHM_ID")));
	int key = ftok("/tmp", port);
	if(key == -1) {
        	perror("ftok fail!\n");
        	return -1;
	}

//	printf("Bind port: %d, key: %d\n", port, key);	
	pid_shmid = shmget(key, sizeof(struct PIDBuf), IPC_CREAT | 0777);
	if(pid_shmid == -1) {
		perror("shmget fail!\n");
		return -1;
	}

	pid_shm = (struct PIDBuf *)shmat(pid_shmid, NULL, 0);
	if(!pid_shm) {
		perror("shmat fail!\n");
		return -1;
	}

	pid_shm->bind_flag = BIND_FLAG;
	pid_shm->pid = pid;
	
//	printf("Bind Success\n");
	return port;
}

void SHMVAN::Connect(const Node& node) 
{
	CHECK_NE(node.id, node.kEmpty);
    	CHECK_NE(node.port, node.kEmpty);

	int id = node.id;

	int key = ftok("/tmp", node.port);
	if(key == -1) {
        	perror("ftok fail!\n");
       	 	return;
	}

//	printf("node.port: %d, key: %d\n", node.port, key);
	int shmid = shmget(key, sizeof(struct PIDBuf), IPC_CREAT | 0777);
	if(shmid == -1) {
		perror("shmget fail!\n");
		return;
	}

	struct PIDBuf *pid_buf = (struct PIDBuf *)shmat(shmid, NULL, 0);
	if(!pid_buf) {
		perror("shmat fail!\n");
		return;
	}

	while(pid_buf->bind_flag != BIND_FLAG);

	int node_pid = pid_buf->pid;

	if(recver_pid.find(id) == recver_pid.end()) {
		recver_pid[id] = node_pid;

		//has connected, only update <id, node_pid>
		if(recver.find(node_pid) != recver.end()) {

			printf("This node has connected, only update <id, node_pid> recored!\n");
			return ;
		}

		std::string buf_path;
		int shmid;
		CreateBufferFile(buf_path, pid, node_pid);
		struct VanBuf *buf = VanBufCreate(buf_path, shmid);
		if(buf) {
			VanBufInit(buf);
			buf->sender_id = my_node_.id;
			recver[node_pid] = std::make_pair(shmid, buf);
		} else {
			printf("VanBuf create fail!\n");
		}

		printf("[%s] my pid: %d, target pid: %d\n", __FUNCTION__, pid, node_pid);
		Notify(SIGCONNECT, node_pid);
		WaitConnected(buf);
	}
//	printf("Connect Success!\n");
}

void SHMVAN::Stop()
{
	Van::Stop();
	Notify(SIGTERMINATE, pid);
	pthread_join(signal_tid, NULL);
	//delete sender
	for(const auto& n : sender) {
		VanBufDestroy(n.second.first, n.second.second);
		DestroyBufferFile(n.first, pid);
	}

	shmdt(pid_shm);
	shmctl(pid_shmid, IPC_RMID, NULL);
	
	recver.clear();
	recver_pid.clear();
}


ssize_t SHMVAN::Recv(struct RingBuffer *ring_buffer, void *buf, size_t len)
{
	ssize_t l = 0, _l;
	
	if(len == 0) return l;

	if(!ring_buffer) return -1;

	while(len > TRANSFER_SIZE) {
		_l = RingBufferGet(ring_buffer, (unsigned char *)buf+l, TRANSFER_SIZE);
		l += _l;
		len -= _l;
	}

	while(len > 0) {
		_l = RingBufferGet(ring_buffer, (unsigned char *)buf+l, len);
		l += _l;
		len -= _l;
	}

	return l;
}

ssize_t SHMVAN::Send(struct RingBuffer *ring_buffer, const void *buf, size_t len)
{
	ssize_t l = 0, _l;

	if(len == 0) return l;

	if(!ring_buffer) {
		return -1;
	}
	
	while(len > TRANSFER_SIZE) {
		_l = RingBufferPut(ring_buffer, (unsigned char *)buf+l, TRANSFER_SIZE);
		l += _l;
		len -= _l;
	}
	
	while(len > 0) {
		_l = RingBufferPut(ring_buffer,(unsigned char *)buf+l, len);
		l += _l;
		len -= _l;
	}
	
	return l;
}

int SHMVAN::SendMsg(const Message& msg) {
  	// find the socket
  	int id = msg.meta.recver;
  	CHECK_NE(id, Meta::kEmpty);
	if(recver_pid.find(id) == recver_pid.end()) {
		printf("Node %d not connect, [%s] fail!\n", id, __FUNCTION__);
		return -1;
	}

	pid_t recv_pid = recver_pid[id];
	struct VanBuf *buf = recver[recv_pid].second;

	int meta_size; char* meta_buf;
  	PackMeta(msg.meta, &meta_buf, &meta_size);
	int size = 0, size_;
	int n = msg.data.size();
	if(n > 64) {
		printf("Send limit exceeded");
		return -1;
	}
	
	buf->meta_size = meta_size;
	buf->data_num = n;

	for(int i = 0; i < n; i++) {
		size_ = msg.data[i].size();
		buf->data_size[i] = size_;
		size += size_;
	}

	struct RingBuffer *ring_buffer = &buf->rb;
	
//	printf("Will SendMsg, recv_pid: %d, send_pid: %d!\n", recv_pid, pid);
	Notify(SIGRECV, recv_pid);
	//send meta
	while (true) {
		if (Send(ring_buffer, meta_buf, meta_size) == meta_size) break;
		printf("WARNING failed to send meta data to node: %d, send size: %d\n", id, meta_size);
		return -1;
	}
	delete meta_buf;
	
  	int send_bytes = meta_size;

	// send data
  	for (int i = 0; i < n; ++i) {
		SArray<char>* data = new SArray<char>(msg.data[i]);
		int data_size = data->size();
		char *data_buf = data->data();
		while (true) {
	  		if (Send(ring_buffer, data_buf, data_size) == data_size) break;
	  		printf("WARNING failed to send meta data to node: %d, send size: %d\n", id, data_size);
	  		return -1;
		}
	
		send_bytes += data_size;
  	}
	
//	printf("Send success, recv_pid: %d, send_pid: %d, size: %d, meta_size: %d, data_num: %d\n", recv_pid, pid, send_bytes, meta_size, n);
  	return send_bytes;
}

int SHMVAN::RecvMsg(Message* msg) 
{
	size_t  meta_size, data_num, len, l;
	msg->data.clear();
	pid_t send_pid = pid_queue.WaitAndPop();
	if(sender.find(send_pid) == sender.end()) {
		printf("[%s] error send pid: %d, receive msg fail!\n", __FUNCTION__, send_pid);
		return -1;
	}
//	printf("Will RecvMsg, recv_pid: %d, send_pid: %d\n", pid, send_pid);
	
	struct VanBuf *buf = sender[send_pid].second;
	struct RingBuffer *ring_buffer = &buf->rb;
	
	meta_size = buf->meta_size;
	data_num = buf->data_num;
	
	msg->meta.sender = buf->sender_id;			
   	msg->meta.recver = my_node_.id;

	char *meta_buf = (char *)malloc(meta_size);
	
	len = Recv(ring_buffer, meta_buf, meta_size);
	if(len != meta_size) {
		printf("Recv meta data fail!\n");
		free(meta_buf);
		return -1;
	}

	UnpackMeta(meta_buf, meta_size, &(msg->meta));
	free(meta_buf);

	for(size_t i = 0; i < data_num; i++) {
		size_t data_size = buf->data_size[i];
		char *data_buf = (char *)malloc(data_size);
		
		l = Recv(ring_buffer, data_buf, data_size);
		if(l != data_size) {
			printf("Recv data fail!\n");
			free(data_buf);
			return -1;
		}
		SArray<char> data;
		data.reset(data_buf, data_size, [](char *data_buf){free(data_buf);});
		msg->data.push_back(data);
		len += l;
	}
	
//	printf("Recv success, recv_pid: %d, send_pid: %d, size: %ld, meta_size: %ld, data_num: %ld\n", pid, send_pid, len, meta_size, data_num);
//	elapse = cpu_second() - start;

//	printf("[%s] times: %.3f, recv size: %ld, bandwidth: %.3fGB/s\n", __FUNCTION__, elapse, len, len / (elapse*1024*1024*1024));

	return len;
}


}

