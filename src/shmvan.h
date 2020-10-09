#ifndef _SHMVAN_H_
#define _SHMVAN_H_

#include <pthread.h>
#include <unordered_map>
#include <sys/types.h>
#include <mutex>
#include <condition_variable>
#include <queue>
#include "ps/internal/van.h"

namespace ps {

#define RINGBUFFER_SIZE		(16*1024*1024)			//64MB

struct RingBuffer {
	unsigned int size;
	unsigned int in;
	unsigned int out;
	pthread_spinlock_t lock;
	unsigned char buf[RINGBUFFER_SIZE];
};

struct VanBuf {
	size_t	meta_size;
	size_t	data_num;					//data_num <= 64
	size_t	data_size[64];
	int sender_id;
	pthread_mutex_t connect_mutex;
	pthread_cond_t connect_cond;
	bool connected_flag;
	struct RingBuffer rb;
};

template <typename T>
class Queue {
public:
	void Push(T val);
	T WaitAndPop();

private:
	std::queue<T> q;
	std::mutex mtx;
	std::condition_variable cv;
};

class SHMVAN : public Van{
public:
	SHMVAN() {}
	virtual ~SHMVAN() {}
	virtual void Start(int customer_id);
	virtual void Stop();
	virtual int Bind(const Node& node, int max_retry);
	virtual void Connect(const Node& node);
	virtual int RecvMsg(Message *msg);
	virtual int SendMsg(const Message &msg);

	void PackMeta(const Meta& meta, char **meta_buf, int *buf_size);
	void UnpackMeta(const char* meta_buf, int buf_size, Meta* meta);

private:
	static void* SignalThread(void *args);
	static void SignalHandle(int signo, siginfo_t *info);
	void SetCurVan();
	void Notify(int signo, int pid);
	void WaitConnected(struct VanBuf *buf);
	void SignalRecv(siginfo_t *info);
	void SignalConnected(siginfo_t *info);
	void SignalConnect(siginfo_t *info);
	ssize_t Recv(struct RingBuffer *ring_buffer, void *buf, size_t len);
	ssize_t Send(struct RingBuffer *ring_buffer, const void *buf, size_t len);

private:
	pid_t pid;															//my pid
	static SHMVAN *cur_van;
	sigset_t mask;
	pthread_t signal_tid;
	
	std::unordered_map<pid_t, std::pair<int, VanBuf *> > sender;		//record sender's <pid, <shmid, VanBuf *> >
	std::unordered_map<pid_t, std::pair<int, VanBuf *> > recver;		//record recver's <pid, <shmid, VanBuf *> >
	std::unordered_map<int, pid_t>						 recver_pid;	//record recver's <node_id, pid>
	
	Queue<pid_t>										 pid_queue;
};

}
#endif


